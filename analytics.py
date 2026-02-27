import sqlite3
import pandas as pd

class PokerAnalytics:
    def __init__(self, db_path='pokernow.db'):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)

    def get_priors(self):
        query = """
        WITH top_names AS (
            SELECT player_id, player_name,
                   ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY COUNT(*) DESC) as rn
            FROM players
            GROUP BY player_id, player_name
        ),
        player_hands AS (
            SELECT player_id, COUNT(DISTINCT hand_id) as total_hands
            FROM events
            GROUP BY player_id
        ),
        vpip_hands AS (
            -- VPIP: Voluntarily put money in pot. Calls or raises preflop.
            SELECT player_id, COUNT(DISTINCT hand_id) as vpip_hands
            FROM events
            WHERE stage = 'Preflop' AND action IN ('call', 'raise', 'raise_to_amount')
              AND NOT action IN ('post_sb', 'post_bb')
            GROUP BY player_id
        ),
        pfr_hands AS (
            -- PFR: Preflop raise
            SELECT player_id, COUNT(DISTINCT hand_id) as pfr_hands
            FROM events
            WHERE stage = 'Preflop' AND action LIKE 'raise%'
            GROUP BY player_id
        ),
        three_bet_hands AS (
            -- 3-Bet: Raise preflop when there is already a raise (simplified heuristic: more than 1 raise in the hand preflop, or raising a raise)
            -- A true 3-bet calculation requires knowing the state of previous actions.
            -- For MVP: We will count a 3-bet if a player raises and there was already a raise in the same hand preflop.
            SELECT e1.player_id, COUNT(DISTINCT e1.hand_id) as three_bet_hands
            FROM events e1
            JOIN events e2 ON e1.hand_id = e2.hand_id
              AND e2.stage = 'Preflop' AND e2.action LIKE 'raise%'
              AND e1.id > e2.id
            WHERE e1.stage = 'Preflop' AND e1.action LIKE 'raise%'
            GROUP BY e1.player_id
        )
        SELECT
            ph.player_id,
            tn.player_name as display_name,
            ph.total_hands,
            COALESCE(v.vpip_hands, 0) as vpip_hands,
            COALESCE(p.pfr_hands, 0) as pfr_hands,
            COALESCE(t.three_bet_hands, 0) as three_bet_hands,
            ROUND(CAST(COALESCE(v.vpip_hands, 0) AS FLOAT) / ph.total_hands * 100, 2) as vpip_pct,
            ROUND(CAST(COALESCE(p.pfr_hands, 0) AS FLOAT) / ph.total_hands * 100, 2) as pfr_pct,
            ROUND(CAST(COALESCE(t.three_bet_hands, 0) AS FLOAT) / ph.total_hands * 100, 2) as three_bet_pct
        FROM player_hands ph
        LEFT JOIN top_names tn ON ph.player_id = tn.player_id AND tn.rn = 1
        LEFT JOIN vpip_hands v ON ph.player_id = v.player_id
        LEFT JOIN pfr_hands p ON ph.player_id = p.player_id
        LEFT JOIN three_bet_hands t ON ph.player_id = t.player_id
        """
        return pd.read_sql_query(query, self.conn)

    def get_profit_loss_by_position(self, player_id):
        query = """
        WITH player_street_investment AS (
            SELECT hand_id, stage, MAX(amount) as street_max
            FROM events
            WHERE player_id = ? AND action IN ('post_sb', 'post_bb', 'post_other', 'call', 'raise', 'bet', 'raise_to_amount')
            GROUP BY hand_id, stage
        ),
        player_investment AS (
            SELECT hand_id, SUM(street_max) as invested
            FROM player_street_investment
            GROUP BY hand_id
        ),
        player_returned AS (
            SELECT hand_id, SUM(amount) as returned
            FROM events
            WHERE player_id = ? AND action = 'returned'
            GROUP BY hand_id
        ),
        player_collected AS (
            SELECT hand_id, SUM(amount) as collected
            FROM events
            WHERE player_id = ? AND action = 'collect'
            GROUP BY hand_id
        ),
        positions AS (
            SELECT hand_id, player_id,
                   RANK() OVER(PARTITION BY hand_id ORDER BY id ASC) as pos_rank
            FROM events
            WHERE stage = 'Preflop' AND (action LIKE 'post_%' OR action IN ('fold','call','raise','check'))
            GROUP BY hand_id, player_id
        ),
        hand_counts AS (
            SELECT hand_id, COUNT(DISTINCT player_id) as num_players
            FROM events
            WHERE stage = 'Preflop' AND (action LIKE 'post_%' OR action IN ('fold','call','raise','check'))
            GROUP BY hand_id
        )
        SELECT
            pi.hand_id,
            pos.pos_rank,
            hc.num_players,
            COALESCE(pc.collected, 0) + COALESCE(pr.returned, 0) - pi.invested as net_profit
        FROM player_investment pi
        LEFT JOIN player_collected pc ON pi.hand_id = pc.hand_id
        LEFT JOIN player_returned pr ON pi.hand_id = pr.hand_id
        LEFT JOIN positions pos ON pi.hand_id = pos.hand_id AND pos.player_id = ?
        LEFT JOIN hand_counts hc ON pi.hand_id = hc.hand_id
        """
        # Note: Invested amounts are slightly inaccurate because "raise" amount is total bet, but good enough for MVP visualization.

        df = pd.read_sql_query(query, self.conn, params=(player_id, player_id, player_id, player_id))

        # Map pos_rank to something like SB, BB, UTG, etc.
        def map_pos(row):
            rank = row['pos_rank']
            n = row['num_players']

            if pd.isna(rank) or pd.isna(n):
                return 'Unknown'

            rank = int(rank)
            n = int(n)

            if n == 2:
                if rank == 1: return 'BTN/SB'
                if rank == 2: return 'BB'
                return f'Pos {rank}'

            if rank == 1: return 'SB'
            if rank == 2: return 'BB'
            if rank == n: return 'BTN'
            if rank == n - 1: return 'CO'
            if rank == n - 2: return 'HJ'

            if rank == 3: return 'UTG'
            if rank == 4: return 'UTG+1' if n >= 8 else 'MP'
            if rank == 5: return 'MP' if n >= 9 else 'MP+1'
            if rank == 6: return 'MP+1'

            return f'Pos {rank}'

        df['position'] = df.apply(map_pos, axis=1)

        result = df.groupby('position')['net_profit'].sum().reset_index()
        # Ensure ordering
        pos_order = ['BTN/SB', 'SB', 'BB', 'UTG', 'UTG+1', 'MP', 'MP+1', 'HJ', 'CO', 'BTN', 'Unknown']
        cat_dtype = pd.CategoricalDtype(categories=[p for p in pos_order if p in result['position'].values]+list(set(result['position'])-set(pos_order)), ordered=True)
        result['position'] = result['position'].astype(cat_dtype)
        return result.sort_values('position')

    def get_bet_sizing_frequencies(self):
        query = """
        WITH top_names AS (
            SELECT player_id, player_name as display_name,
                   ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY COUNT(*) DESC) as rn
            FROM players
            GROUP BY player_id, player_name
        )
        SELECT
            e.player_id,
            tn.display_name,
            e.amount,
            e.pot_size
        FROM events e
        LEFT JOIN top_names tn ON e.player_id = tn.player_id AND tn.rn = 1
        WHERE e.stage IN ('Flop', 'Turn', 'River') AND e.action IN ('bet', 'raise', 'raise_to_amount') AND e.pot_size > 0
        """
        df = pd.read_sql_query(query, self.conn)

        if df.empty:
            return df

        # Calculate bet size relative to pot
        df['pct_of_pot'] = df['amount'] / df['pot_size']

        def categorize_bet(pct):
            if pct < 0.33: return 'Small (<33%)'
            elif pct <= 0.66: return 'Medium (33-66%)'
            else: return 'Large (>66%)'

        df['bet_size_category'] = df['pct_of_pot'].apply(categorize_bet)

        # We can group by player_id and display_name
        return df.groupby(['player_id', 'bet_size_category']).size().unstack(fill_value=0)


    def _normalize_hole_cards(self, cards_str):
        import pandas as pd
        if pd.isna(cards_str) or not cards_str:
            return 'Unknown'
        cards = cards_str.split(',')
        if len(cards) != 2:
            return 'Unknown'
        
        ranks = [c[0] for c in cards]
        suits = [c[1] for c in cards]
        
        rank_order = {'A': 14, 'K': 13, 'Q': 12, 'J': 11, 'T': 10, '9': 9, '8': 8, '7': 7, '6': 6, '5': 5, '4': 4, '3': 3, '2': 2}
        
        try:
            r1, r2 = ranks[0], ranks[1]
            if rank_order[r1] < rank_order[r2]:
                r1, r2 = r2, r1
                s1, s2 = suits[1], suits[0]
            else:
                s1, s2 = suits[0], suits[1]
        except KeyError:
            return cards_str
            
        if r1 == r2:
            return f"{r1}{r2}"
        else:
            suited = 's' if s1 == s2 else 'o'
            return f"{r1}{r2}{suited}"

    def get_pnl_by_hand(self, player_id):
        query = '''
        WITH player_street_investment AS (
            SELECT hand_id, stage, MAX(amount) as street_max
            FROM events
            WHERE player_id = ? AND action IN ('post_sb', 'post_bb', 'post_other', 'call', 'raise', 'bet', 'raise_to_amount')
            GROUP BY hand_id, stage
        ),
        player_investment AS (
            SELECT hand_id, SUM(street_max) as invested
            FROM player_street_investment
            GROUP BY hand_id
        ),
        player_returned AS (
            SELECT hand_id, SUM(amount) as returned
            FROM events
            WHERE player_id = ? AND action = 'returned'
            GROUP BY hand_id
        ),
        player_collected AS (
            SELECT hand_id, SUM(amount) as collected
            FROM events
            WHERE player_id = ? AND action = 'collect'
            GROUP BY hand_id
        ),
        player_pnl AS (
            SELECT
                pi.hand_id,
                COALESCE(pc.collected, 0) + COALESCE(pr.returned, 0) - pi.invested as net_profit
            FROM player_investment pi
            LEFT JOIN player_collected pc ON pi.hand_id = pc.hand_id
            LEFT JOIN player_returned pr ON pi.hand_id = pr.hand_id
        )
        SELECT 
            ph.hole_cards,
            COUNT(*) as times_dealt,
            SUM(pnl.net_profit) as total_pnl
        FROM player_hand_cards ph
        JOIN player_pnl pnl ON ph.hand_id = pnl.hand_id
        WHERE ph.player_id = ?
        GROUP BY ph.hole_cards
        ORDER BY total_pnl DESC
        '''
        df = pd.read_sql_query(query, self.conn, params=(player_id, player_id, player_id, player_id))
        if df.empty:
            return df
            
        df['hand_combo'] = df['hole_cards'].apply(self._normalize_hole_cards)
        summary = df.groupby('hand_combo').agg(
            times_dealt=('times_dealt', 'sum'),
            total_pnl=('total_pnl', 'sum')
        ).reset_index()
        return summary.sort_values('total_pnl', ascending=False)

    def get_positional_stats(self, player_id):
        query = '''
        WITH positions AS (
            SELECT hand_id, player_id,
                   RANK() OVER(PARTITION BY hand_id ORDER BY id ASC) as pos_rank
            FROM events
            WHERE stage = 'Preflop' AND (action LIKE 'post_%' OR action IN ('fold','call','raise','check'))
            GROUP BY hand_id, player_id
        ),
        hand_counts AS (
            SELECT hand_id, COUNT(DISTINCT player_id) as num_players
            FROM events
            WHERE stage = 'Preflop' AND (action LIKE 'post_%' OR action IN ('fold','call','raise','check'))
            GROUP BY hand_id
        ),
        player_actions AS (
            SELECT hand_id,
                   MAX(CASE WHEN action IN ('call', 'raise', 'raise_to_amount') AND NOT action IN ('post_sb', 'post_bb') THEN 1 ELSE 0 END) as vpip_flag,
                   MAX(CASE WHEN action LIKE 'raise%' THEN 1 ELSE 0 END) as pfr_flag
            FROM events
            WHERE player_id = ? AND stage = 'Preflop'
            GROUP BY hand_id
        ),
        three_bet AS (
            SELECT e1.hand_id, 1 as three_bet_flag
            FROM events e1
            JOIN events e2 ON e1.hand_id = e2.hand_id
              AND e2.stage = 'Preflop' AND e2.action LIKE 'raise%'
              AND e1.id > e2.id
            WHERE e1.player_id = ? AND e1.stage = 'Preflop' AND e1.action LIKE 'raise%'
            GROUP BY e1.hand_id
        )
        SELECT 
            pos.pos_rank, 
            hc.num_players,
            COUNT(pos.hand_id) as total_hands,
            SUM(COALESCE(pa.vpip_flag, 0)) as vpip_hands,
            SUM(COALESCE(pa.pfr_flag, 0)) as pfr_hands,
            SUM(COALESCE(tb.three_bet_flag, 0)) as three_bet_hands
        FROM positions pos
        LEFT JOIN hand_counts hc ON pos.hand_id = hc.hand_id
        LEFT JOIN player_actions pa ON pos.hand_id = pa.hand_id
        LEFT JOIN three_bet tb ON pos.hand_id = tb.hand_id
        WHERE pos.player_id = ?
        GROUP BY pos.pos_rank, hc.num_players
        '''
        import pandas as pd
        df = pd.read_sql_query(query, self.conn, params=(player_id, player_id, player_id))
        
        if df.empty:
            return df
            
        def map_pos(row):
            rank = row['pos_rank']
            n = row['num_players']
            if pd.isna(rank) or pd.isna(n):
                return 'Unknown'
            rank, n = int(rank), int(n)
            
            if n == 2:
                if rank == 1: return 'BTN/SB'
                if rank == 2: return 'BB'
                return f'Pos {rank}'
            if rank == 1: return 'SB'
            if rank == 2: return 'BB'
            if rank == n: return 'BTN'
            if rank == n - 1: return 'CO'
            if rank == n - 2: return 'HJ'
            if rank == 3: return 'UTG'
            if rank == 4: return 'UTG+1' if n >= 8 else 'MP'
            if rank == 5: return 'MP' if n >= 9 else 'MP+1'
            if rank == 6: return 'MP+1'
            return f'Pos {rank}'

        df['position'] = df.apply(map_pos, axis=1)
        
        result = df.groupby('position').agg(
            total_hands=('total_hands', 'sum'),
            vpip_hands=('vpip_hands', 'sum'),
            pfr_hands=('pfr_hands', 'sum'),
            three_bet_hands=('three_bet_hands', 'sum')
        ).reset_index()
        
        result['vpip_pct'] = (result['vpip_hands'] / result['total_hands'] * 100).round(2)
        result['pfr_pct'] = (result['pfr_hands'] / result['total_hands'] * 100).round(2)
        result['three_bet_pct'] = (result['three_bet_hands'] / result['total_hands'] * 100).round(2)
        
        pos_order = ['BTN/SB', 'SB', 'BB', 'UTG', 'UTG+1', 'MP', 'MP+1', 'HJ', 'CO', 'BTN', 'Unknown']
        cat_dtype = pd.CategoricalDtype(categories=[p for p in pos_order if p in result['position'].values]+list(set(result['position'])-set(pos_order)), ordered=True)
        result['position'] = result['position'].astype(cat_dtype)
        return result.sort_values('position')


    def get_net_pnl_all_players(self):
        query = """
        WITH player_street_investment AS (
            SELECT hand_id, player_id, stage, MAX(amount) as street_max
            FROM events
            WHERE action IN ('post_sb', 'post_bb', 'post_other', 'call', 'raise', 'bet', 'raise_to_amount')
            GROUP BY hand_id, player_id, stage
        ),
        player_total_investment AS (
            SELECT player_id, SUM(street_max) as total_invested
            FROM player_street_investment
            GROUP BY player_id
        ),
        player_total_returned AS (
            SELECT player_id, SUM(amount) as total_returned
            FROM events
            WHERE action = 'returned'
            GROUP BY player_id
        ),
        player_total_collected AS (
            SELECT player_id, SUM(amount) as total_collected
            FROM events
            WHERE action = 'collect'
            GROUP BY player_id
        ),
        top_names AS (
            SELECT player_id, player_name as display_name,
                   ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY COUNT(*) DESC) as rn
            FROM players
            GROUP BY player_id, player_name
        )
        SELECT
            tn.player_id,
            tn.display_name,
            COALESCE(pc.total_collected, 0) + COALESCE(pr.total_returned, 0) - COALESCE(pi.total_invested, 0) as total_net_pnl
        FROM top_names tn
        LEFT JOIN player_total_investment pi ON tn.player_id = pi.player_id
        LEFT JOIN player_total_collected pc ON tn.player_id = pc.player_id
        LEFT JOIN player_total_returned pr ON tn.player_id = pr.player_id
        WHERE tn.rn = 1
        ORDER BY total_net_pnl DESC
        """
        return pd.read_sql_query(query, self.conn)

if __name__ == "__main__":
    analytics = PokerAnalytics()
    print("=== Priors ===")
    print(analytics.get_priors())
    print("\n=== Post-flop Bet Sizing Frequencies ===")
    print(analytics.get_bet_sizing_frequencies())
    print("\n=== Profit/Loss By Position for 'Me' ===")
    # Just grab the first ID for testing
    first_id = analytics.get_priors()['player_id'].iloc[0]
    print(f"Testing for ID: {first_id}")
    print(analytics.get_profit_loss_by_position(first_id))
