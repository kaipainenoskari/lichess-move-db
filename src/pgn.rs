// Stream PGN parse + game filter + emit (fen, rating_band, move_uci, result)
use anyhow::Result;
use pgn_reader::{RawTag, Reader, SanPlus, Visitor};
use shakmaty::{CastlingMode, Color, Chess, EnPassantMode, Position, fen::Fen};
use std::io::BufReader;
use std::ops::ControlFlow;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct MoveRecord {
    pub fen: String,
    pub rating_band: u32,
    pub move_uci: String,
    pub games: u64,
    pub wins: u64,
    pub draws: u64,
}

#[derive(Default, Clone)]
struct GameTags {
    event: Option<String>,
    white_elo: Option<u32>,
    black_elo: Option<u32>,
    result: Option<GameResult>,
    fen: Option<String>,
}

#[derive(Clone, Copy)]
enum GameResult {
    WhiteWin,
    BlackWin,
    Draw,
}

impl std::str::FromStr for GameResult {
    type Err = ();
    fn from_str(s: &str) -> std::result::Result<Self, ()> {
        match s.trim() {
            "1-0" => Ok(GameResult::WhiteWin),
            "0-1" => Ok(GameResult::BlackWin),
            "1/2-1/2" | "*" => Ok(GameResult::Draw),
            _ => Err(()),
        }
    }
}

/// Movetext = position + game tags (so we have Elo and result in san())
type Movetext = (Chess, GameTags);

struct FenVisitor<F> {
    time_controls: Vec<String>,
    rating_min: u32,
    rating_max: u32,
    band_width: u32,
    callback: F,
}

impl<F: FnMut(MoveRecord)> Visitor for FenVisitor<F> {
    type Tags = GameTags;
    type Movetext = Movetext;
    type Output = ();

    fn begin_tags(&mut self) -> ControlFlow<Self::Output, Self::Tags> {
        ControlFlow::Continue(GameTags::default())
    }

    fn tag(
        &mut self,
        tags: &mut Self::Tags,
        name: &[u8],
        value: RawTag<'_>,
    ) -> ControlFlow<Self::Output> {
        let value_str = String::from_utf8_lossy(value.as_bytes()).trim().to_string();
        match name {
            b"Event" => tags.event = Some(value_str),
            b"WhiteElo" => {
                if let Ok(n) = value_str.parse::<u32>() {
                    tags.white_elo = Some(n);
                }
            }
            b"BlackElo" => {
                if let Ok(n) = value_str.parse::<u32>() {
                    tags.black_elo = Some(n);
                }
            }
            b"Result" => {
                tags.result = value_str.parse().ok();
            }
            b"FEN" => tags.fen = Some(value_str),
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn begin_movetext(&mut self, tags: Self::Tags) -> ControlFlow<Self::Output, Self::Movetext> {
        let event_ok = tags
            .event
            .as_ref()
            .map(|e| self.time_controls.iter().any(|tc| e.contains(tc)))
            == Some(true);
        let elo_ok = tags.white_elo.is_some() && tags.black_elo.is_some();
        let result_ok = tags.result.is_some();
        if !event_ok || !elo_ok || !result_ok {
            return ControlFlow::Break(());
        }
        let pos = match &tags.fen {
            Some(fen_str) => {
                let fen: Fen = match fen_str.parse() {
                    Ok(f) => f,
                    Err(_) => return ControlFlow::Break(()),
                };
                match fen.into_position(CastlingMode::Standard) {
                    Ok(p) => p,
                    Err(_) => return ControlFlow::Break(()),
                }
            }
            None => Chess::default(),
        };
        ControlFlow::Continue((pos, tags))
    }

    fn san(&mut self, movetext: &mut Self::Movetext, san_plus: SanPlus) -> ControlFlow<Self::Output> {
        let (pos, tags) = movetext;
        let m = match san_plus.san.to_move(pos) {
            Ok(m) => m,
            Err(_) => return ControlFlow::Break(()),
        };
        let fen = Fen::from_position(pos, EnPassantMode::Legal)
            .to_string()
            .trim()
            .to_string();
        let turn = pos.turn();
        let white_elo = tags.white_elo.unwrap();
        let black_elo = tags.black_elo.unwrap();
        let rating = match turn {
            Color::White => white_elo,
            Color::Black => black_elo,
        };
        let band = (rating / self.band_width) * self.band_width;
        if band < self.rating_min || band > self.rating_max {
            let new_pos = match pos.clone().play(m) {
                Ok(p) => p,
                Err(_) => return ControlFlow::Break(()),
            };
            *pos = new_pos;
            return ControlFlow::Continue(());
        }
        let move_uci = m.to_uci(pos.castles().mode()).to_string();
        let (wins, draws) = match tags.result.unwrap() {
            GameResult::WhiteWin => match turn {
                Color::White => (1u64, 0u64),
                Color::Black => (0u64, 0u64),
            },
            GameResult::BlackWin => match turn {
                Color::White => (0u64, 0u64),
                Color::Black => (1u64, 0u64),
            },
            GameResult::Draw => (0u64, 1u64),
        };
        (self.callback)(MoveRecord {
            fen,
            rating_band: band,
            move_uci,
            games: 1,
            wins,
            draws,
        });
        let new_pos = match pos.clone().play(m) {
            Ok(p) => p,
            Err(_) => return ControlFlow::Break(()),
        };
        *pos = new_pos;
        ControlFlow::Continue(())
    }

    fn end_game(&mut self, _movetext: Self::Movetext) -> Self::Output {
        ()
    }
}

/// Process one .pgn.zst file: stream decompress, parse games, filter, call callback for each move record.
/// If max_games is Some(n), stop after n games and return early. Returns the number of games processed.
pub fn process_file<F: FnMut(MoveRecord)>(
    path: &Path,
    time_controls: &[String],
    rating_min: u32,
    rating_max: u32,
    band_width: u32,
    max_games: Option<u64>,
    mut on_record: F,
) -> Result<u64> {
    let file = std::fs::File::open(path)?;
    let decoder = zstd::Decoder::new(file)?;
    let reader = BufReader::new(decoder);
    let mut visitor = FenVisitor {
        time_controls: time_controls.to_vec(),
        rating_min,
        rating_max,
        band_width,
        callback: &mut on_record,
    };
    let mut pgn_reader = Reader::new(reader);
    let mut games_processed = 0u64;
    while pgn_reader.read_game(&mut visitor)?.is_some() {
        games_processed += 1;
        if max_games.map_or(false, |n| games_processed >= n) {
            break;
        }
    }
    Ok(games_processed)
}
