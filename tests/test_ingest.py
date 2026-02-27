import unittest
import sqlite3
import os
import sys

# Add parent directory to sys.path so we can import ingest
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingest import init_db

class TestIngest(unittest.TestCase):
    def setUp(self):
        # We use an in-memory database for testing
        self.db_path = ':memory:'
        self.conn = init_db(self.db_path)
        self.cursor = self.conn.cursor()

    def tearDown(self):
        self.conn.close()

    def test_init_db_creates_tables(self):
        # Verify that all required tables are created
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in self.cursor.fetchall()]

        expected_tables = [
            'hands',
            'players',
            'player_hand_cards',
            'events',
            'player_priors',
            'sqlite_sequence' # Auto-created for AUTOINCREMENT
        ]

        for table in expected_tables:
            self.assertIn(table, tables, f"Table {table} was not created.")

    def test_schema_integrity(self):
        # Test basic insertion and retrieval without relying on strict SQLite PRAGMA configurations
        self.cursor.execute('INSERT INTO players (player_id, player_name) VALUES (?, ?)', ('p1', 'Alice'))
        self.cursor.execute('INSERT INTO hands (hand_id, dealer_name, started_at) VALUES (?, ?, ?)', ('h1', 'p1', '2023-01-01T00:00:00'))
        self.cursor.execute('''
            INSERT INTO events (hand_id, player_id, action, amount, pot_size, stage, timestamp, raw_entry)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', ('h1', 'p1', 'fold', 0, 0, 'Preflop', '2023-01-01T00:00:00', '{}'))
        self.conn.commit()

        self.cursor.execute('SELECT COUNT(*) FROM players')
        player_count = self.cursor.fetchone()[0]
        self.assertEqual(player_count, 1)

        self.cursor.execute('SELECT COUNT(*) FROM events')
        event_count = self.cursor.fetchone()[0]
        self.assertEqual(event_count, 1)

if __name__ == '__main__':
    unittest.main()
