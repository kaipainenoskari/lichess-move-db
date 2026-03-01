-- FEN move statistics: one row per (fen, rating_band, move) with raw counts.
-- Rating band is the Elo band of the side to move (e.g. 1600 for 100-width bands).
CREATE TABLE IF NOT EXISTS fen_move_stats (
    fen TEXT NOT NULL,
    rating_band INTEGER NOT NULL,
    move TEXT NOT NULL,
    games INTEGER NOT NULL,
    wins INTEGER NOT NULL,
    draws INTEGER NOT NULL,
    PRIMARY KEY (fen, rating_band, move)
);
CREATE INDEX IF NOT EXISTS idx_fen_rating ON fen_move_stats(fen, rating_band);

-- Months already processed (avoid double-counting on incremental runs).
CREATE TABLE IF NOT EXISTS processed_months (
    month TEXT PRIMARY KEY
);
