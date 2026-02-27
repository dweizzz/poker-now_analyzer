import unittest
import pandas as pd
import sys
import os
import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analytics import PokerAnalytics
from ingest import init_db

class TestAnalytics(unittest.TestCase):
    def setUp(self):
        # We use an in-memory database and pass the mocked connection to Analytics
        self.db_path = ':memory:'
        self.conn = init_db(self.db_path)

        # Override the PokerAnalytics connection
        self.analytics = PokerAnalytics(db_path=self.db_path)
        # Hack to replace the conn with our in-memory one
        # Because PokerAnalytics internally creates a new connection, we'll patch it:
        self.analytics.conn = self.conn

        # Setup mock data
        self.cursor = self.conn.cursor()
        self._seed_mock_data()

    def tearDown(self):
        self.conn.close()

    def _seed_mock_data(self):
        # Insert Players
        players = [
            ('p1', 'Alice'),
            ('p2', 'Bob'),
            ('p3', 'Charlie')
        ]
        self.cursor.executemany("INSERT INTO players (player_id, player_name) VALUES (?, ?)", players)

        # Insert Hands
        hands = [
            ('h1', 'p3', '2023-01-01T00:00:00'), # Dealer is p3 (BTN), p1 SB, p2 BB
            ('h2', 'p1', '2023-01-01T00:05:00'), # Dealer is p1 (BTN), p2 SB, p3 BB
            ('h3', 'p2', '2023-01-01T00:10:00'), # Dealer is p2 (BTN), p3 SB, p1 BB
            ('h4', 'p3', '2023-01-01T00:15:00')  # Dealer is p3 (BTN), p1 SB, p2 BB
        ]
        self.cursor.executemany("INSERT INTO hands (hand_id, dealer_name, started_at) VALUES (?, ?, ?)", hands)

        # Insert Hole Cards Let's say for p1 only to test get_pnl_by_hand
        self.cursor.execute("INSERT INTO player_hand_cards (hand_id, player_id, hole_cards) VALUES ('h1', 'p1', 'As,Ks')")
        self.cursor.execute("INSERT INTO player_hand_cards (hand_id, player_id, hole_cards) VALUES ('h2', 'p1', 'Jd,Th')")

        # --- Events ---

        events = []

        def add_event(hand_id, player_id, action, amount=0, pot_size=0, stage='Preflop'):
            event_id = len(events) + 1
            # Default empty json for raw_entry just in case
            raw_entry = "{}"
            # Let's add mock showdown string for 'show'
            if action == 'show' and player_id == 'p1':
               raw_entry = json.dumps({"hand": {"name": "Pair"}})
            elif action == 'show' and player_id == 'p2':
               raw_entry = json.dumps({"hand": {"name": "Two Pair"}})

            events.append((event_id, hand_id, player_id, action, amount, pot_size, stage, '2023', raw_entry))

        # Hand 1: Simple Preflop Raise and Fold. p1 SB (0.5), p2 BB (1). p3 folds. p1 raises to 3. p2 folds. p1 collects.
        add_event('h1', 'p1', 'post_sb', 0.5, 0.5)
        add_event('h1', 'p2', 'post_bb', 1.0, 1.5)
        add_event('h1', 'p3', 'fold', 0, 1.5)
        add_event('h1', 'p1', 'raise_to_amount', 3.0, 4.0) # VPIP, PFR
        add_event('h1', 'p2', 'fold', 0, 4.0)
        # Uncalled amount returned to p1
        add_event('h1', 'p1', 'returned', 2.0, 2.0)
        # p1 collects the pot
        add_event('h1', 'p1', 'collect', 2.0, 2.0, 'Showdown') # Actually no showdown, just collect

        # Hand 2: 3-Bet Preflop. p2 SB (0.5), p3 BB (1). p1 (BTN) raises to 3. p2 3-bets to 9. p3 folds. p1 folds. p2 collects.
        add_event('h2', 'p2', 'post_sb', 0.5, 0.5)
        add_event('h2', 'p3', 'post_bb', 1.0, 1.5)
        add_event('h2', 'p1', 'raise_to_amount', 3.0, 4.5) # VPIP, PFR
        add_event('h2', 'p2', 'raise_to_amount', 9.0, 13.5) # VPIP, PFR, 3-Bet
        add_event('h2', 'p3', 'fold', 0, 13.5)
        add_event('h2', 'p1', 'fold', 0, 13.5)
        add_event('h2', 'p2', 'returned', 6.0, 7.5) # p2 gets the uncalled 6 back
        add_event('h2', 'p2', 'collect', 4.5, 0, 'Showdown') # 0.5 (p2 sb) + 1 (p3 bb) + 3 (p1 raise) = 4.5

        # Hand 3: Showdown. p3 SB (0.5), p1 BB (1). p2 (BTN) calls 1. p3 calls 0.5. p1 checks. (Pot: 3).
        add_event('h3', 'p3', 'post_sb', 0.5, 0.5)
        add_event('h3', 'p1', 'post_bb', 1.0, 1.5)
        add_event('h3', 'p2', 'call', 1.0, 2.5) # VPIP
        add_event('h3', 'p3', 'call', 0.5, 3.0) # VPIP
        add_event('h3', 'p1', 'check', 0, 3.0)
        # Flop Deal
        add_event('h3', 'Dealer', 'deal_flop', 0, 3.0, 'Flop')
        add_event('h3', 'p3', 'check', 0, 3.0, 'Flop')
        add_event('h3', 'p1', 'check', 0, 3.0, 'Flop')
        add_event('h3', 'p2', 'bet', 2.0, 5.0, 'Flop') # Bet sizing: 2/3 pot (Medium)
        add_event('h3', 'p3', 'fold', 0, 5.0, 'Flop')
        add_event('h3', 'p1', 'call', 2.0, 7.0, 'Flop')
        # Turn Deal
        add_event('h3', 'Dealer', 'deal_turn', 0, 7.0, 'Turn')
        add_event('h3', 'p1', 'check', 0, 7.0, 'Turn')
        add_event('h3', 'p2', 'check', 0, 7.0, 'Turn')
        # River Deal
        add_event('h3', 'Dealer', 'deal_river', 0, 7.0, 'River')
        add_event('h3', 'p1', 'bet', 15.0, 22.0, 'River') # Bet sizing: 15/22 = 68% (Large).
        add_event('h3', 'p2', 'call', 15.0, 37.0, 'River')
        # Showdown
        add_event('h3', 'p1', 'show', 0, 37.0, 'Showdown')
        add_event('h3', 'p2', 'show', 0, 37.0, 'Showdown')
        add_event('h3', 'p2', 'collect', 37.0, 0, 'Showdown') # p2 Wins at showdown

        # Hand 4: River Bluff Frequency. p1 SB (0.5), p2 BB (1), p3 BTN calls 1.
        add_event('h4', 'p1', 'post_sb', 0.5, 0.5)
        add_event('h4', 'p2', 'post_bb', 1.0, 1.5)
        add_event('h4', 'p3', 'call', 1.0, 2.5)
        add_event('h4', 'p1', 'call', 0.5, 3.0)
        add_event('h4', 'p2', 'check', 0, 3.0)
        add_event('h4', 'Dealer', 'deal_flop', 0, 3.0, 'Flop')
        add_event('h4', 'p1', 'check', 0, 3.0, 'Flop')
        add_event('h4', 'p2', 'check', 0, 3.0, 'Flop')
        add_event('h4', 'p3', 'check', 0, 3.0, 'Flop')
        add_event('h4', 'Dealer', 'deal_turn', 0, 3.0, 'Turn')
        add_event('h4', 'p1', 'check', 0, 3.0, 'Turn')
        add_event('h4', 'p2', 'check', 0, 3.0, 'Turn')
        add_event('h4', 'p3', 'check', 0, 3.0, 'Turn')
        add_event('h4', 'Dealer', 'deal_river', 0, 3.0, 'River')
        # Bluff
        add_event('h4', 'p3', 'bet', 3.0, 6.0, 'River') # p3 bluffs at the river
        add_event('h4', 'p1', 'call', 3.0, 9.0, 'River')
        add_event('h4', 'p2', 'fold', 0, 9.0, 'River')
        add_event('h4', 'p1', 'show', 0, 9.0, 'Showdown')
        add_event('h4', 'p3', 'show', 0, 9.0, 'Showdown')
        add_event('h4', 'p1', 'collect', 9.0, 0, 'Showdown') # p1 wins, so p3's raise was a bluff

        self.cursor.executemany('''
            INSERT INTO events
            (id, hand_id, player_id, action, amount, pot_size, stage, timestamp, raw_entry)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', events)

        self.conn.commit()

    def test_get_priors(self):
        df = self.analytics.get_priors()
        self.assertFalse(df.empty)

        p1 = df[df['player_id'] == 'p1'].iloc[0]
        # p1 played 4 hands.
        # h1: raise_to_amount (VPIP, PFR)
        # h2: raise_to_amount (VPIP, PFR)
        # h3: called bb, then called bb (Call - VPIP? BB check doesn't count, but h3 p1 calls later post-flop? No, VPIP is preflop only. In h3 p1 posts BB, then checks. VPIP=False)
        # WAIT: In h3, p1 posts BB, then checks. "NOT action IN ('post_sb', 'post_bb')". So h3 is not VPIP.
        # WAIT: In h4, p1 posts SB, then calls 0.5. This IS VPIP.
        # p1 VPIP: h1, h2, h4 = 3
        # p1 PFR: h1, h2 = 2
        # p1 3-bet: 0
        self.assertEqual(p1['total_hands'], 4)
        self.assertEqual(p1['vpip_hands'], 3)
        self.assertEqual(p1['pfr_hands'], 2)
        self.assertEqual(p1['three_bet_hands'], 0)

        p2 = df[df['player_id'] == 'p2'].iloc[0]
        # p2 hands: 4.
        # h1: BB, folded. VPIP 0.
        # h2: SB, 3-bet. VPIP 1, PFR 1, 3Bet 1.
        # h3: BTN, call. VPIP 1.
        # h4: BB, check. VPIP 0.
        # p2 VPIP: 2
        # p2 PFR: 1
        # p2 3-bet: 1
        self.assertEqual(p2['vpip_hands'], 2)
        self.assertEqual(p2['pfr_hands'], 1)
        self.assertEqual(p2['three_bet_hands'], 1)

    def test_get_profit_loss_by_position(self):
        # We test p1
        df = self.analytics.get_profit_loss_by_position('p1')
        self.assertFalse(df.empty)

        # In h1: p1 is SB. Invests 3 (SB 0.5, raise 3 = max 3). returned 2, collect 2. net = 2 + 2 - 3 = 1
        # In h2: p1 is BTN. Invests 3. Returned 0, collect 0. net = -3
        # In h3: p1 is BB. Preflop max 1. Flop max 2. River max 5. Total invested = 1 + 2 + 5 = 8. Collect 0. net = -8
        # In h4: p1 is SB. Preflop max 1. Total = 1. Net = -1.

        # Groupings based on position: 3 players
        # From map_pos: n=3. rank 1=SB, 2=BB, 3=BTN.

        # SB: h1 (+1), h4 (+5.5). Sum = 6.5
        # BB: h3 (-18). Sum = -18
        # BTN: h2 (-3). Sum = -3

        sb_pnl = df[df['position'] == 'SB']['net_profit'].sum()
        self.assertEqual(sb_pnl, 6.5)

        bb_pnl = df[df['position'] == 'BB']['net_profit'].sum()
        self.assertEqual(bb_pnl, -18)

        btn_pnl = df[df['position'] == 'BTN']['net_profit'].sum()
        self.assertEqual(btn_pnl, -3)

    def test_get_bet_sizing_frequencies(self):
        df = self.analytics.get_bet_sizing_frequencies()
        self.assertFalse(df.empty)
        # Check if column structure is right
        self.assertIn('Large (>66%)', df.columns)
        self.assertIn('Medium (33-66%)', df.columns)

        # In h3: p2 bets 2 into pot 3 (Flop) -> 66.6% -> Medium
        p2_medium = df.loc['p2', 'Medium (33-66%)']
        self.assertEqual(p2_medium, 1)

        # In h3: p1 bets 15 into pot 22 (River) -> 68% -> Large
        p1_large = df.loc['p1', 'Large (>66%)']
        self.assertEqual(p1_large, 1)

    def test_get_pnl_by_hand(self):
        df = self.analytics.get_pnl_by_hand('p1')
        self.assertFalse(df.empty)
        # h1 p1 had As,Ks, PNL = +1
        # h2 p1 had Jd,Th, PNL = -3

        row_ak = df[df['hand_combo'] == 'AKs'].iloc[0]
        self.assertEqual(row_ak['total_pnl'], 1.0)

        row_jt = df[df['hand_combo'] == 'JTo'].iloc[0]
        self.assertEqual(row_jt['total_pnl'], -3.0)

    def test_get_positional_stats(self):
        df = self.analytics.get_positional_stats('p2')
        self.assertFalse(df.empty)
        # p2: h1 (BB, folds), h2 (SB, 3bet), h3 (BTN, calls preflop), h4 (BB, checks)
        # SB hands=1, VPIP=1, PFR=1, 3bet=1
        # BB hands=2, VPIP=0, PFR=0
        # BTN hands=1, VPIP=1, PFR=0

        sb_stats = df[df['position'] == 'SB'].iloc[0]
        self.assertEqual(sb_stats['total_hands'], 1)
        self.assertEqual(sb_stats['vpip_hands'], 1)
        self.assertEqual(sb_stats['three_bet_hands'], 1)

        bb_stats = df[df['position'] == 'BB'].iloc[0]
        self.assertEqual(bb_stats['total_hands'], 2)
        self.assertEqual(bb_stats['vpip_hands'], 0)

    def test_get_net_pnl_all_players(self):
        df = self.analytics.get_net_pnl_all_players()
        self.assertFalse(df.empty)

        # Let's check p1 overall net: +1 (h1) -3 (h2) -18 (h3) +5.5 (h4) = -14.5
        p1_net = df[df['player_id'] == 'p1'].iloc[0]['total_net_pnl']
        self.assertEqual(p1_net, -14.5)

    def test_calculate_and_store_player_priors(self):
        # We run the calculation method, it should populate the `player_priors` table
        self.analytics.calculate_and_store_player_priors()

        # Check values
        df = pd.read_sql_query("SELECT * FROM player_priors", self.conn)
        self.assertEqual(len(df), 4) # 3 players + Dealer

        # Analyze p1 post-flop
        p1 = df[df['player_id'] == 'p1'].iloc[0]
        # p1 saw flops: h3, h4. Total = 2.
        # Showdowns seen: h3, h4. Total = 2.
        # WTSD% = 2/2 = 100%
        # Flops won: h4 (River win). Total = 1.
        # WSD% = 1/2 = 50%
        # WWSF% = 1/2 = 50%
        self.assertEqual(p1['wtsd_pct'], 100.0)
        self.assertEqual(p1['wsd_pct'], 50.0)
        self.assertEqual(p1['wwsf_pct'], 50.0)

        # Analyze p2 post-flop
        p2 = df[df['player_id'] == 'p2'].iloc[0]
        # p2 saw flops: h3, h4. Total = 2.
        # Showdowns seen: h3. Total = 1.
        # Won flops: h2 (preflop won, no flop), h3 (showdown win). So flops won = 1.
        self.assertEqual(p2['wtsd_pct'], 50.0)
        self.assertEqual(p2['wsd_pct'], 100.0)
        self.assertEqual(p2['wwsf_pct'], 50.0)

        # Analyze p3 post-flop
        p3 = df[df['player_id'] == 'p3'].iloc[0]
        # p3 saw flop: h4. (h1 folded pre, h2 folded pre, h3 folded flop). Total = 1
        # River bluff opportunities: h4. (River raise/bet and won without showdown) -> 1 bluff!
        self.assertEqual(p3['river_bluff_freq'], 100.0)

    def test_get_exploit_targets(self):
        self.analytics.calculate_and_store_player_priors()

        # Insert dummy players with >= 50 hands since the mock DB only has ~4 hands
        self.cursor.execute("INSERT INTO players (player_id, player_name) VALUES ('dummy1', 'Dummy One')")
        self.cursor.execute("INSERT INTO players (player_id, player_name) VALUES ('dummy2', 'Dummy Two')")
        self.cursor.execute('''
            INSERT INTO player_priors
            (player_id, total_hands, vpip_pct, pfr_pct, three_bet_pct, wtsd_pct, wsd_pct, wwsf_pct, river_bluff_freq, avg_showdown_strength, profile_tag)
            VALUES ('dummy1', 50, 20.0, 15.0, 5.0, 30.0, 50.0, 45.0, 10.0, 2.5, 'Regular')
        ''')
        self.cursor.execute('''
            INSERT INTO player_priors
            (player_id, total_hands, vpip_pct, pfr_pct, three_bet_pct, wtsd_pct, wsd_pct, wwsf_pct, river_bluff_freq, avg_showdown_strength, profile_tag)
            VALUES ('dummy2', 60, 20.0, 15.0, 5.0, 40.0, 50.0, 45.0, 10.0, 2.5, 'Regular')
        ''')
        self.conn.commit()

        df = self.analytics.get_exploit_targets()
        self.assertFalse(df.empty)
        self.assertEqual(len(df), 2) # Should only return dummy1 and dummy2

        self.assertIn('wtsd_pct', df.columns)
        self.assertIn('pfr_pct', df.columns)
        # Check sort order by wtsd_pct DESC
        self.assertTrue(df.iloc[0]['wtsd_pct'] >= df.iloc[-1]['wtsd_pct'])

    def test_get_hero_leaks(self):
        self.analytics.calculate_and_store_player_priors()
        result = self.analytics.get_hero_leaks('p1')
        self.assertIsNotNone(result)
        self.assertIn('stats', result)
        self.assertIn('leaks', result)

        # Because p1 WTSD% is 50%, it should trigger the Calling Station leak message
        leaks = result['leaks']
        self.assertTrue(any("Calling Station" in l for l in leaks))

if __name__ == '__main__':
    unittest.main()
