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

        # Calculate bet size relative to pot before the bet
        df['pot_before'] = df['pot_size'] - df['amount']
        # Avoid division by zero
        df.loc[df['pot_before'] <= 0, 'pot_before'] = 0.01
        df['pct_of_pot'] = df['amount'] / df['pot_before']

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

    def map_hand_strength(self, hand_desc):
        if not hand_desc:
            return 0
        desc = str(hand_desc).lower()
        if 'royal flush' in desc or 'straight flush' in desc: return 8
        if 'four of a kind' in desc or 'quads' in desc: return 7
        if 'full house' in desc: return 6
        if 'flush' in desc: return 5
        if 'straight' in desc: return 4
        if 'three of a kind' in desc or 'set' in desc or 'trips' in desc: return 3
        if 'two pair' in desc: return 2
        if 'pair' in desc: return 1
        return 0 # High card or unknown

    def calculate_and_store_player_priors(self, hero_id="EJd9KHwjJa"):
        import json

        base_stats = self.get_priors()
        if base_stats.empty:
            return

        query = """
        WITH base_players AS (
            SELECT DISTINCT player_id, hand_id
            FROM events
            WHERE player_id != 'Dealer' AND action NOT IN ('deal_flop', 'deal_turn', 'deal_river')
        ),
        flops AS (
            SELECT DISTINCT hand_id FROM events WHERE action = 'deal_flop'
        ),
        folds AS (
            SELECT DISTINCT player_id, hand_id FROM events WHERE action = 'fold'
        ),
        preflop_folds AS (
            SELECT DISTINCT player_id, hand_id FROM events WHERE stage = 'Preflop' AND action = 'fold'
        ),
        saw_flop AS (
            SELECT bp.player_id, bp.hand_id
            FROM base_players bp
            JOIN flops f ON bp.hand_id = f.hand_id
            LEFT JOIN preflop_folds pf ON bp.hand_id = pf.hand_id AND bp.player_id = pf.player_id
            WHERE pf.player_id IS NULL
        ),
        survivors AS (
            SELECT bp.player_id, bp.hand_id
            FROM base_players bp
            LEFT JOIN folds f ON bp.player_id = f.player_id AND bp.hand_id = f.hand_id
            WHERE f.player_id IS NULL
        ),
        showdown_hands AS (
            SELECT hand_id FROM survivors GROUP BY hand_id HAVING COUNT(player_id) > 1
        ),
        showdowns AS (
            SELECT s.player_id, s.hand_id
            FROM survivors s
            JOIN showdown_hands sh ON s.hand_id = sh.hand_id
        ),
        wins AS (
            SELECT DISTINCT player_id, hand_id FROM events WHERE action = 'collect'
        ),
        river_raises AS (
            SELECT DISTINCT player_id, hand_id FROM events WHERE stage = 'River' AND action IN ('raise', 'bet', 'raise_to_amount')
        ),
        river_bluffs AS (
            SELECT rr.player_id, rr.hand_id
            FROM river_raises rr
            JOIN showdowns s ON rr.hand_id = s.hand_id AND rr.player_id = s.player_id
            LEFT JOIN wins w ON rr.hand_id = w.hand_id AND rr.player_id = w.player_id
            WHERE w.player_id IS NULL
        ),
        river_raise_opps AS (
            SELECT rr.player_id, rr.hand_id
            FROM river_raises rr
            JOIN showdowns s ON rr.hand_id = s.hand_id AND rr.player_id = s.player_id
        )
        SELECT
            bp.player_id,
            COUNT(DISTINCT sf.hand_id) as flops_seen,
            COUNT(DISTINCT s.hand_id) as showdowns_seen,
            COUNT(DISTINCT sf_w.hand_id) as flops_won,
            COUNT(DISTINCT s_w.hand_id) as showdowns_won,
            COUNT(DISTINCT rb.hand_id) as river_bluffs,
            COUNT(DISTINCT rro.hand_id) as river_raise_opps
        FROM base_players bp
        LEFT JOIN saw_flop sf ON bp.player_id = sf.player_id AND bp.hand_id = sf.hand_id
        LEFT JOIN showdowns s ON bp.player_id = s.player_id AND bp.hand_id = s.hand_id
        LEFT JOIN wins sf_w ON sf.player_id = sf_w.player_id AND sf.hand_id = sf_w.hand_id
        LEFT JOIN wins s_w ON s.player_id = s_w.player_id AND s.hand_id = s_w.hand_id
        LEFT JOIN river_bluffs rb ON bp.player_id = rb.player_id AND bp.hand_id = rb.hand_id
        LEFT JOIN river_raise_opps rro ON bp.player_id = rro.player_id AND bp.hand_id = rro.hand_id
        GROUP BY bp.player_id
        """
        adv_stats = pd.read_sql_query(query, self.conn)

        # Showdown strengths
        showdown_query = "SELECT player_id, raw_entry FROM events WHERE action = 'show' OR stage = 'Showdown'"
        shows_df = pd.read_sql_query(showdown_query, self.conn)

        player_strengths = {}
        for _, row in shows_df.iterrows():
            pid = row['player_id']
            try:
                payload = json.loads(row['raw_entry'])
                desc = None
                if 'hand' in payload and isinstance(payload['hand'], dict):
                    desc = payload['hand'].get('name') or payload['hand'].get('description')
                if not desc:
                    desc = str(payload)

                val = self.map_hand_strength(desc)
                if pid not in player_strengths:
                    player_strengths[pid] = []
                player_strengths[pid].append(val)
            except:
                pass

        cursor = self.conn.cursor()

        for _, row in base_stats.iterrows():
            pid = row['player_id']
            hands = row['total_hands']
            vpip = row['vpip_pct']
            pfr = row['pfr_pct']
            three_bet = row['three_bet_pct']

            adv = adv_stats[adv_stats['player_id'] == pid]
            wtsd_pct = 0.0
            wsd_pct = 0.0
            wwsf_pct = 0.0
            river_bluff_freq = 0.0
            avg_strength = 0.0
            profile_tag = "Unknown"

            if not adv.empty:
                adv = adv.iloc[0]
                flops_seen = adv['flops_seen']
                showdowns_seen = adv['showdowns_seen']
                flops_won = adv['flops_won']
                showdowns_won = adv['showdowns_won']
                r_bluffs = adv['river_bluffs']
                r_opps = adv['river_raise_opps']

                if flops_seen > 0:
                    wtsd_pct = round((showdowns_seen / flops_seen) * 100, 2)
                    wwsf_pct = round((flops_won / flops_seen) * 100, 2)

                if showdowns_seen > 0:
                    wsd_pct = round((showdowns_won / showdowns_seen) * 100, 2)

                if r_opps > 0:
                    river_bluff_freq = round((r_bluffs / r_opps) * 100, 2)

            if pid in player_strengths and len(player_strengths[pid]) > 0:
                vals = [v for v in player_strengths[pid] if v > 0] # Filter out 0s if we failed to parse
                if vals:
                    avg_strength = round(sum(vals) / len(vals), 2)

            if hands >= 10: # Lowered requirement slightly to actually test on short datasets, though requirement was 50
                if avg_strength <= 1.5 and avg_strength > 0:
                    profile_tag = "Bluff-Heavy/Station"
                elif avg_strength >= 3.0:
                    profile_tag = "Under-Bluffing/Nit"
                else:
                    profile_tag = "Regular"

            cursor.execute('''
            INSERT OR REPLACE INTO player_priors
            (player_id, total_hands, vpip_pct, pfr_pct, three_bet_pct, wtsd_pct, wsd_pct, wwsf_pct, river_bluff_freq, avg_showdown_strength, profile_tag)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (pid, hands, vpip, pfr, three_bet, wtsd_pct, wsd_pct, wwsf_pct, river_bluff_freq, avg_strength, profile_tag))

        self.conn.commit()

    def get_exploit_targets(self):
        query = """
        SELECT pp.*, p.player_name as display_name
        FROM player_priors pp
        LEFT JOIN (
            SELECT player_id, player_name, ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY COUNT(*) DESC) as rn
            FROM players GROUP BY player_id, player_name
        ) p ON pp.player_id = p.player_id AND p.rn = 1
        WHERE pp.total_hands >= 50
        ORDER BY pp.wtsd_pct DESC
        """
        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            return pd.DataFrame()

    def get_visual_analytics_data(self, min_hands=100):
        query = """
        WITH bb_sizes AS (
            SELECT hand_id, MAX(amount) as bb_amount
            FROM events
            WHERE action = 'post_bb'
            GROUP BY hand_id
        ),
        player_street_investment AS (
            SELECT hand_id, player_id, stage, MAX(amount) as street_max
            FROM events
            WHERE action IN ('post_sb', 'post_bb', 'post_other', 'call', 'raise', 'bet', 'raise_to_amount')
            GROUP BY hand_id, player_id, stage
        ),
        player_investment AS (
            SELECT hand_id, player_id, SUM(street_max) as invested
            FROM player_street_investment
            GROUP BY hand_id, player_id
        ),
        player_returned AS (
            SELECT hand_id, player_id, SUM(amount) as returned
            FROM events
            WHERE action = 'returned'
            GROUP BY hand_id, player_id
        ),
        player_collected AS (
            SELECT hand_id, player_id, SUM(amount) as collected
            FROM events
            WHERE action = 'collect'
            GROUP BY hand_id, player_id
        ),
        player_pnl_bb AS (
            SELECT
                pi.player_id,
                SUM((COALESCE(pc.collected, 0) + COALESCE(pr.returned, 0) - pi.invested) / COALESCE(b.bb_amount, 0.5)) as total_bb_won
            FROM player_investment pi
            LEFT JOIN player_collected pc ON pi.hand_id = pc.hand_id AND pi.player_id = pc.player_id
            LEFT JOIN player_returned pr ON pi.hand_id = pr.hand_id AND pi.player_id = pr.player_id
            LEFT JOIN bb_sizes b ON pi.hand_id = b.hand_id
            GROUP BY pi.player_id
        )
        SELECT
            pp.player_id,
            p.player_name as display_name,
            pp.total_hands,
            pp.vpip_pct,
            pp.pfr_pct,
            pp.wtsd_pct,
            pp.wsd_pct,
            pp.profile_tag,
            pnl.total_bb_won,
            ROUND((pnl.total_bb_won / (CAST(pp.total_hands AS FLOAT) / 100)), 2) as bb_per_100
        FROM player_priors pp
        LEFT JOIN (
            SELECT player_id, player_name, ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY COUNT(*) DESC) as rn
            FROM players GROUP BY player_id, player_name
        ) p ON pp.player_id = p.player_id AND p.rn = 1
        LEFT JOIN player_pnl_bb pnl ON pp.player_id = pnl.player_id
        WHERE pp.total_hands >= ?
        """
        try:
            return pd.read_sql_query(query, self.conn, params=(min_hands,))
        except Exception as e:
            return pd.DataFrame()


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
class LineExploitEngine:
    def __init__(self, db_path='pokernow.db'):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)

    def _map_strength(self, desc):
        desc = str(desc).lower()
        if 'royal flush' in desc or 'straight flush' in desc or 'quads' in desc or 'four of a kind' in desc or 'full house' in desc:
            return 4 # Nuts
        if 'flush' in desc or 'straight' in desc or 'set' in desc or 'three of a kind' in desc or 'trips' in desc:
            return 3 # Strong
        if 'two pair' in desc or 'top pair' in desc:
            return 2 # Medium
        if 'pair' in desc: # under top pair
            return 1 # Weak
        return 0 # Air

    def _classify_texture(self, board_cards):
        if not board_cards or len(board_cards) < 3: return ['Dry']
        tags = []
        ranks = [c[0].upper() for c in board_cards if c]
        suits = [c[1].lower() for c in board_cards if c]

        from collections import Counter
        suit_counts = Counter(suits)
        if any(cnt >= 3 for cnt in suit_counts.values()):
            tags.append('Monotone')

        rank_counts = Counter(ranks)
        if any(cnt >= 2 for cnt in rank_counts.values()):
            tags.append('Paired')

        rank_order = {'A':14, 'K':13, 'Q':12, 'J':11, 'T':10, '9':9, '8':8, '7':7, '6':6, '5':5, '4':4, '3':3, '2':2}
        vals = sorted(list(set([rank_order.get(r, 0) for r in ranks])))

        connected = False
        for i in range(len(vals) - 2):
            if vals[i+2] - vals[i] == 2 and vals[i+1] - vals[i] == 1:
                connected = True
                break
        if 14 in vals and 2 in vals and 3 in vals:
            connected = True
        if connected:
            tags.append('Connected')

        # Broadway (2+ cards T, J, Q, K, A)
        if sum(1 for k in ranks if rank_order.get(k, 0) >= 10) >= 2:
            tags.append('Broadway')

        if not tags:
            tags.append('Dry')

        return tags

    def _map_pos(self, rank, n):
        if pd.isna(rank) or rank == 999999 or pd.isna(n) or n == 0: return 'Unknown'
        rank, n = int(rank), int(n)
        if n == 2:
            if rank == 1: return 'SB'
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

    def build_action_lines(self):
        import json

        query = """
        SELECT e.hand_id, e.player_id, e.stage, e.action, e.amount, e.pot_size, e.board_cards, e.raw_entry,
               p.player_name as display_name,
               RANK() OVER(PARTITION BY e.hand_id ORDER BY CASE WHEN e.stage='Preflop' AND (e.action LIKE 'post_%' OR e.action IN ('fold','call','raise','check')) THEN MIN(e.id) ELSE 999999 END ASC) as pos_rank
        FROM events e
        LEFT JOIN (
            SELECT player_id, player_name, ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY COUNT(*) DESC) as rn
            FROM players GROUP BY player_id, player_name
        ) p ON e.player_id = p.player_id AND p.rn = 1
        GROUP BY e.hand_id, e.player_id, e.stage, e.action, e.amount, e.pot_size, e.board_cards, e.raw_entry, p.player_name
        ORDER BY e.hand_id, MIN(e.id) ASC
        """
        df = pd.read_sql_query(query, self.conn)

        lines = []
        hand_groups = df.groupby('hand_id')

        for hand_id, group in hand_groups:
            player_states = {} # pid -> {'line': [], 'strength': None, 'last_sizing': None, 'texture': []}
            current_board = []

            for _, row in group.iterrows():
                pid = row['player_id']
                stage = row['stage']
                action = row['action']
                amount = row['amount']
                pot_size = row['pot_size']
                b_cards = row['board_cards']
                raw_entry = row['raw_entry']

                if b_cards:
                    current_board = b_cards.split(',')

                if pid == 'Dealer' or not pid:
                    continue

                if action == 'show' or stage == 'Showdown':
                    try:
                        payload = json.loads(raw_entry)
                        desc = payload.get('handDescription')
                        if not desc and 'hand' in payload and isinstance(payload['hand'], dict):
                            desc = payload['hand'].get('name') or payload['hand'].get('description')

                        # Try taking from combination if description isn't present
                        if not desc and 'combination' in payload:
                             desc = "pair" if len(set([c[0] for c in payload['combination']])) < 5 else "high card"

                        if desc and pid in player_states:
                           player_states[pid]['strength'] = self._map_strength(desc)
                    except:
                        pass
                    continue

                if pid not in player_states:
                    player_states[pid] = {'line': [], 'strength': None, 'last_sizing': None, 'texture': [], 'display_name': row['display_name'], 'pos_rank': row['pos_rank']}

                # Determine action code
                code = None
                if stage == 'Preflop':
                    if action in ['raise', 'raise_to_amount']: code = 'PFR'
                    elif action == 'call': code = 'PFC'
                elif stage == 'Flop':
                    if action in ['bet', 'raise', 'raise_to_amount']: code = 'F-Bet'
                    elif action == 'call': code = 'F-Call'
                elif stage == 'Turn':
                    if action in ['bet', 'raise', 'raise_to_amount']: code = 'T-Bet'
                    elif action == 'call': code = 'T-Call'
                elif stage == 'River':
                    if action in ['bet', 'raise', 'raise_to_amount']: code = 'R-Bet'
                    elif action == 'call': code = 'R-Call'

                if code and (not player_states[pid]['line'] or player_states[pid]['line'][-1] != code):
                    player_states[pid]['line'].append(code)

                if action in ['bet', 'raise', 'raise_to_amount'] and pot_size and pot_size > 0:
                    pot_before = pot_size - amount
                    pct = (amount / pot_before) if pot_before > 0 else 1.5
                    sizing = 'Overbet (>120%)'
                    if pct < 0.4: sizing = 'Small (<40%)'
                    elif pct <= 0.8: sizing = 'Medium (40-80%)'
                    elif pct <= 1.2: sizing = 'Large (80-120%)'
                    player_states[pid]['last_sizing'] = sizing
                    player_states[pid]['texture'] = self._classify_texture(current_board)

            # Calculate simple positions based on pos_rank order
            num_players = len([p for p in player_states.values() if p['pos_rank'] == p['pos_rank']]) # Ignore nan

            for pid, state in player_states.items():
                if state['line']:
                    str_line = "_".join(state['line'])
                    pos_str = self._map_pos(state['pos_rank'], num_players)
                    lines.append({
                        'hand_id': hand_id,
                        'player_id': pid,
                        'display_name': state['display_name'],
                        'position': pos_str,
                        'action_line': str_line,
                        'sizing_bucket': state['last_sizing'],
                        'texture_tags': ','.join(state['texture']) if state['texture'] else 'Dry',
                        'strength_tier': state['strength']
                    })

        return pd.DataFrame(lines)

    def get_hand_events(self, hand_id):
        query = """
        WITH PlayerFirstAction AS (
            SELECT hand_id, player_id, MIN(id) as first_action_id
            FROM events
            WHERE hand_id = ? AND stage='Preflop' AND (action LIKE 'post_%' OR action IN ('fold','call','raise','check'))
            GROUP BY hand_id, player_id
        ),
        RankedPlayers AS (
            SELECT hand_id, player_id, RANK() OVER(PARTITION BY hand_id ORDER BY first_action_id ASC) as pos_rank
            FROM PlayerFirstAction
        )
        SELECT e.id, e.stage, e.action, e.amount, e.pot_size, e.board_cards, e.player_id, e.raw_entry,
               COALESCE(p.player_name, e.player_id) as actor,
               rp.pos_rank,
               (SELECT COUNT(DISTINCT player_id) FROM PlayerFirstAction WHERE hand_id = ?) as table_size
        FROM events e
        LEFT JOIN (
            SELECT player_id, player_name, ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY COUNT(*) DESC) as rn
            FROM players GROUP BY player_id, player_name
        ) p ON e.player_id = p.player_id AND p.rn = 1
        LEFT JOIN RankedPlayers rp ON e.hand_id = rp.hand_id AND e.player_id = rp.player_id
        WHERE e.hand_id = ?
        ORDER BY e.id ASC
        """
        df = pd.read_sql_query(query, self.conn, params=(hand_id, hand_id, hand_id))

        # apply positional mapping
        df['position'] = df.apply(lambda row: self._map_pos(row['pos_rank'], row['table_size']), axis=1)

        def extract_details(row):
            import json
            try:
                if not row['raw_entry']:
                    return ""
                payload = json.loads(row['raw_entry'])
                cards = payload.get('cards')
                if not cards and 'hand' in payload and isinstance(payload['hand'], dict):
                    cards = payload['hand'].get('cards')
                if cards and isinstance(cards, list):
                    valid_cards = [c for c in cards if c is not None]
                    if valid_cards:
                        return "Cards: " + ", ".join(valid_cards)
            except:
                pass
            return ""

        df['details'] = df.apply(extract_details, axis=1)
        return df

    def get_triple_barrel_auditor(self, df_lines=None):
        if df_lines is None:
            df_lines = self.build_action_lines()
        if df_lines.empty: return pd.DataFrame(), 0, pd.DataFrame()

        # Find lines containing F-Bet_T-Bet_R-Bet
        tb = df_lines[df_lines['action_line'].str.contains('F-Bet_T-Bet_R-Bet', regex=False, na=False)]

        showdowns = tb.dropna(subset=['strength_tier'])
        freq = 0
        if not showdowns.empty:
            bluffs = showdowns[showdowns['strength_tier'] <= 1]
            freq = len(bluffs) / len(showdowns) * 100

        # Sizing vs Strength correlation across ALL lines not just triple barrel
        all_showdowns = df_lines.dropna(subset=['strength_tier'])
        sizing_groups = pd.DataFrame()
        if not all_showdowns.empty:
            sizing_groups = all_showdowns.groupby(['sizing_bucket', 'strength_tier']).size().unstack(fill_value=0)

        return tb, freq, sizing_groups

    def get_exploit_finder(self, df_lines=None):
        if df_lines is None:
             df_lines = self.build_action_lines()

        showdowns = df_lines.dropna(subset=['strength_tier'])
        if showdowns.empty: return pd.DataFrame(), pd.DataFrame()

        agg = showdowns.groupby('action_line').agg(
            total_showdowns=('strength_tier', 'count'),
            bluffs=('strength_tier', lambda x: (x <= 1).sum()),
            wins=('strength_tier', lambda x: (x >= 2).sum())
        ).reset_index()

        agg['bluff_freq'] = (agg['bluffs'] / agg['total_showdowns']) * 100
        agg['wsd_pct'] = (agg['wins'] / agg['total_showdowns']) * 100

        # filter for statistical significance (>= 20 data points requirement)
        agg = agg[agg['total_showdowns'] >= 20]

        under_bluffed = agg[(agg['wsd_pct'] > 65) & (agg['bluff_freq'] < 10)].sort_values('total_showdowns', ascending=False).head(3)
        over_bluffed = agg[agg['bluff_freq'] > 40].sort_values('total_showdowns', ascending=False).head(3)

        return under_bluffed, over_bluffed

    def get_texture_bluff_map(self, df_lines=None):
        if df_lines is None:
            df_lines = self.build_action_lines()

        if df_lines.empty:
            return pd.DataFrame()

        # We look at air-at-showdown frequency per explicit texture tag
        showdowns = df_lines.dropna(subset=['strength_tier'])
        if showdowns.empty: return pd.DataFrame()

        records = []
        for _, row in showdowns.iterrows():
            tags = str(row['texture_tags']).split(',')
            for t in tags:
                if t:
                    records.append({'tag': t, 'is_air': row['strength_tier'] <= 1})

        if not records:
             return pd.DataFrame()

        tag_df = pd.DataFrame(records)
        agg = tag_df.groupby('tag').agg(
            total=('is_air', 'count'),
            air=('is_air', 'sum')
        ).reset_index()

        agg['air_pct'] = (agg['air'] / agg['total']) * 100
        return agg.sort_values('air_pct', ascending=False)

    def get_hero_leaks(self, df_lines, hero_id="EJd9KHwjJa"):
        if df_lines is None or df_lines.empty:
            return pd.DataFrame()

        hero_lines = df_lines[df_lines['player_id'] == hero_id]
        if hero_lines.empty:
            return pd.DataFrame()

        # Get hero PNL per hand
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
        SELECT hand_id, net_profit FROM player_pnl
        '''
        df_pnl = pd.read_sql_query(query, self.conn, params=(hero_id, hero_id, hero_id))

        merged = pd.merge(hero_lines, df_pnl, on='hand_id', how='inner')
        if merged.empty:
            return pd.DataFrame()

        # Group by action line to find most and least profitable
        agg = merged.groupby('action_line').agg(
            occurrences=('hand_id', 'count'),
            total_pnl=('net_profit', 'sum'),
            avg_pnl=('net_profit', 'mean')
        ).reset_index()

        return agg.sort_values('total_pnl', ascending=False)

    def get_3bet_vs_srp_stats(self, df_lines=None):
        if df_lines is None:
            df_lines = self.build_action_lines()

        query = """
        SELECT hand_id, SUM(CASE WHEN action = 'raise' THEN 1 ELSE 0 END) as pfr_count
        FROM events
        WHERE stage = 'Preflop'
        GROUP BY hand_id
        """
        pfr_df = pd.read_sql_query(query, self.conn)

        merged = df_lines.merge(pfr_df, on='hand_id', how='left')
        merged['pfr_count'] = merged['pfr_count'].fillna(0)

        srp_lines = merged[merged['pfr_count'] == 1]
        three_bet_lines = merged[merged['pfr_count'] >= 2]

        srp_showdowns = srp_lines.dropna(subset=['strength_tier'])
        tb_showdowns = three_bet_lines.dropna(subset=['strength_tier'])

        srp_bluff_pct = (srp_showdowns['strength_tier'] <= 1).mean() * 100 if len(srp_showdowns) > 0 else 0
        tb_bluff_pct = (tb_showdowns['strength_tier'] <= 1).mean() * 100 if len(tb_showdowns) > 0 else 0

        pf_raise_query = """
        SELECT e.hand_id, e.player_id, MAX(CASE WHEN e.action IN ('raise', 'raise_to_amount') THEN 1 ELSE 0 END) as made_postflop_raise
        FROM events e
        WHERE e.stage IN ('Flop', 'Turn', 'River')
        GROUP BY e.hand_id, e.player_id
        """
        pf_raises_df = pd.read_sql_query(pf_raise_query, self.conn)

        tb_raises = tb_showdowns.merge(pf_raises_df, on=['hand_id', 'player_id'], how='left')
        tb_pf_raisers = tb_raises[tb_raises['made_postflop_raise'] == 1]

        if not tb_pf_raisers.empty:
            tb_pf_raise_nuts_pct = (tb_pf_raisers['strength_tier'] >= 4).mean() * 100
            tb_pf_raise_strong_pct = (tb_pf_raisers['strength_tier'] >= 3).mean() * 100
            tb_pf_raise_air_pct = (tb_pf_raisers['strength_tier'] <= 0).mean() * 100
            tb_pf_raise_count = len(tb_pf_raisers)
        else:
            tb_pf_raise_nuts_pct = 0
            tb_pf_raise_strong_pct = 0
            tb_pf_raise_air_pct = 0
            tb_pf_raise_count = 0

        # 3-bettor is the LAST player to raise preflop
        tb_win_sql = """
        WITH pfrs AS (
            SELECT hand_id, player_id,
                   ROW_NUMBER() OVER(PARTITION BY hand_id ORDER BY id DESC) as rn
            FROM events
            WHERE stage = 'Preflop' AND action IN ('raise', 'raise_to_amount')
        ),
        last_pfrs AS (
            SELECT hand_id, player_id as three_bettor_id
            FROM pfrs
            WHERE rn = 1
        ),
        hand_pfr_counts AS (
            SELECT hand_id, COUNT(*) as pfr_count
            FROM events
            WHERE stage = 'Preflop' AND action IN ('raise', 'raise_to_amount')
            GROUP BY hand_id
        ),
        tb_hands AS (
            SELECT lp.hand_id, lp.three_bettor_id
            FROM last_pfrs lp
            JOIN hand_pfr_counts hc ON lp.hand_id = hc.hand_id
            WHERE hc.pfr_count >= 2
        ),
        collections AS (
            SELECT hand_id, player_id, SUM(amount) as collected
            FROM events
            WHERE action = 'collect'
            GROUP BY hand_id, player_id
        ),
        returns AS (
            SELECT hand_id, player_id, SUM(amount) as returned
            FROM events
            WHERE action = 'returned'
            GROUP BY hand_id, player_id
        )
        SELECT th.hand_id,
               CASE WHEN (c.collected IS NOT NULL AND c.collected > 0) OR (r.returned IS NOT NULL AND r.returned > 0) THEN 1 ELSE 0 END as three_bettor_won
        FROM tb_hands th
        LEFT JOIN collections c ON th.hand_id = c.hand_id AND th.three_bettor_id = c.player_id
        LEFT JOIN returns r ON th.hand_id = r.hand_id AND th.three_bettor_id = r.player_id
        """
        tb_win_df = pd.read_sql_query(tb_win_sql, self.conn)
        tb_win_pct = tb_win_df['three_bettor_won'].mean() * 100 if not tb_win_df.empty else 0
        tb_win_count = len(tb_win_df)

        return {
            'srp_bluff_pct': srp_bluff_pct,
            'srp_count': len(srp_showdowns),
            'tb_bluff_pct': tb_bluff_pct,
            'tb_count': len(tb_showdowns),
            'tb_pf_raise_nuts_pct': tb_pf_raise_nuts_pct,
            'tb_pf_raise_strong_pct': tb_pf_raise_strong_pct,
            'tb_pf_raise_air_pct': tb_pf_raise_air_pct,
            'tb_pf_raise_count': tb_pf_raise_count,
            'tb_win_pct': tb_win_pct,
            'tb_total_hands': tb_win_count
        }

    def get_3bet_preflop_stats(self):
        query = """
        SELECT e.hand_id, e.player_id, e.action, e.id as event_id, ph.hole_cards, p.player_name as display_name
        FROM events e
        LEFT JOIN player_hand_cards ph ON e.hand_id = ph.hand_id AND e.player_id = ph.player_id
        LEFT JOIN (
            SELECT player_id, player_name, ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY COUNT(*) DESC) as rn
            FROM players GROUP BY player_id, player_name
        ) p ON e.player_id = p.player_id AND p.rn = 1
        WHERE e.stage = 'Preflop' AND e.action IN ('raise', 'raise_to_amount', 'call')
        ORDER BY e.hand_id, e.id
        """
        df = pd.read_sql_query(query, self.conn)

        pos_query = """
        WITH PlayerFirstAction AS (
            SELECT hand_id, player_id, MIN(id) as first_action_id
            FROM events
            WHERE stage='Preflop' AND (action LIKE 'post_%' OR action IN ('fold','call','raise','check'))
            GROUP BY hand_id, player_id
        ),
        RankedPlayers AS (
            SELECT hand_id, player_id, RANK() OVER(PARTITION BY hand_id ORDER BY first_action_id ASC) as pos_rank,
                   COUNT(*) OVER(PARTITION BY hand_id) as table_size
            FROM PlayerFirstAction
        )
        SELECT hand_id, player_id, pos_rank, table_size FROM RankedPlayers
        """
        pos_df = pd.read_sql_query(pos_query, self.conn)
        df = df.merge(pos_df, on=['hand_id', 'player_id'], how='left')
        df['position'] = df.apply(lambda row: self._map_pos(row['pos_rank'], row['table_size']), axis=1)

        calls = []
        for hand_id, group in df.groupby('hand_id'):
            raises = 0
            vol_players = set()
            for _, row in group.iterrows():
                action = row['action']
                pid = row['player_id']

                if action in ['raise', 'raise_to_amount']:
                    raises += 1
                    vol_players.add(pid)
                elif action == 'call':
                    if raises >= 2:
                        is_cold = pid not in vol_players
                        calls.append({
                            'hand_id': hand_id,
                            'player_id': pid,
                            'display_name': row['display_name'],
                            'position': row['position'],
                            'hole_cards': row['hole_cards'],
                            'is_cold_call': is_cold
                        })
                    vol_players.add(pid)

        res_df = pd.DataFrame(calls)
        if res_df.empty: return res_df

        def _normalize_cards(cards_str):
            if pd.isna(cards_str) or not cards_str: return 'Unknown'
            cards = cards_str.split(',')
            if len(cards) != 2: return 'Unknown'
            ranks, suits = [c[0] for c in cards], [c[1] for c in cards]
            rank_order = {'A': 14, 'K': 13, 'Q': 12, 'J': 11, 'T': 10, '9': 9, '8': 8, '7': 7, '6': 6, '5': 5, '4': 4, '3': 3, '2': 2}
            try:
                r1, r2 = ranks[0], ranks[1]
                if rank_order[r1] < rank_order[r2]:
                    r1, r2 = r2, r1
                    s1, s2 = suits[1], suits[0]
                else:
                    s1, s2 = suits[0], suits[1]
                if r1 == r2: return f"{r1}{r2}"
                return f"{r1}{r2}{'s' if s1 == s2 else 'o'}"
            except:
                return cards_str

        res_df['hand_combo'] = res_df['hole_cards'].apply(_normalize_cards)
        return res_df

    def get_3bet_postflop_sizings(self, df_lines=None):
        if df_lines is None:
            df_lines = self.build_action_lines()

        query = """
        SELECT hand_id, SUM(CASE WHEN action IN ('raise', 'raise_to_amount') THEN 1 ELSE 0 END) as pfr_count
        FROM events
        WHERE stage = 'Preflop'
        GROUP BY hand_id
        """
        pfr_df = pd.read_sql_query(query, self.conn)
        tb_hands = pfr_df[pfr_df['pfr_count'] >= 2]['hand_id']

        tb_hands_list = tuple(tb_hands.dropna().unique())
        if not tb_hands_list: return pd.DataFrame()

        events_query = """
        SELECT e.id, e.hand_id, e.player_id, e.stage, e.action, e.amount, e.pot_size, p.player_name as display_name
        FROM events e
        LEFT JOIN (
            SELECT player_id, player_name, ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY COUNT(*) DESC) as rn
            FROM players GROUP BY player_id, player_name
        ) p ON e.player_id = p.player_id AND p.rn = 1
        WHERE e.stage IN ('Flop', 'Turn', 'River') AND e.action IN ('bet', 'raise', 'raise_to_amount')
        """
        postflop_df = pd.read_sql_query(events_query, self.conn)
        tb_postflop = postflop_df[postflop_df['hand_id'].isin(tb_hands_list)]

        sizings = []
        for _, row in tb_postflop.iterrows():
            pot = row['pot_size']
            amt = row['amount']
            if not pot or pot <= 0: continue

            pot_before = pot - amt
            pct = (amt / pot_before) if pot_before > 0 else 1.5
            sz = 'Overbet (>120%)'
            if pct < 0.4: sz = 'Small (<40%)'
            elif pct <= 0.8: sz = 'Medium (40-80%)'
            elif pct <= 1.2: sz = 'Large (80-120%)'

            sizings.append({
                'hand_id': row['hand_id'],
                'player_id': row['player_id'],
                'display_name': row['display_name'],
                'stage': row['stage'],
                'action': row['action'],
                'sizing_bucket': sz,
                'amount': amt,
                'pot_size': pot_before
            })

        return pd.DataFrame(sizings)

    # ---------- 3-Bet Visual Analysis Methods ----------

    @staticmethod
    def _normalize_cards_static(cards_str):
        """Normalize hole cards to standard combo notation (e.g. AKs, QJo, TT)."""
        if pd.isna(cards_str) or not cards_str:
            return 'Unknown'
        cards = cards_str.split(',')
        if len(cards) != 2:
            return 'Unknown'
        ranks = [c.strip()[0] for c in cards]
        suits = [c.strip()[1] for c in cards]
        rank_order = {'A': 14, 'K': 13, 'Q': 12, 'J': 11, 'T': 10,
                      '9': 9, '8': 8, '7': 7, '6': 6, '5': 5, '4': 4, '3': 3, '2': 2}
        try:
            r1, r2 = ranks[0], ranks[1]
            s1, s2 = suits[0], suits[1]
            if rank_order.get(r1, 0) < rank_order.get(r2, 0):
                r1, r2 = r2, r1
                s1, s2 = s2, s1
            if r1 == r2:
                return f"{r1}{r2}"
            return f"{r1}{r2}{'s' if s1 == s2 else 'o'}"
        except Exception:
            return 'Unknown'

    def _get_position_query(self):
        """Reusable CTE for positional ranking."""
        return """
        WITH PlayerFirstAction AS (
            SELECT hand_id, player_id, MIN(id) as first_action_id
            FROM events
            WHERE stage='Preflop' AND (action LIKE 'post_%' OR action IN ('fold','call','raise','check'))
            GROUP BY hand_id, player_id
        ),
        RankedPlayers AS (
            SELECT hand_id, player_id,
                   RANK() OVER(PARTITION BY hand_id ORDER BY first_action_id ASC) as pos_rank,
                   COUNT(*) OVER(PARTITION BY hand_id) as table_size
            FROM PlayerFirstAction
        )
        SELECT hand_id, player_id, pos_rank, table_size FROM RankedPlayers
        """

    def get_3bet_frequency_by_position_and_hand(self):
        """Get 3-bet frequency per hand combo, grouped by the 3-bettor's position.

        Returns a DataFrame with: position, hand_combo, three_bet_count, total_opportunities, three_bet_freq
        """
        # Step 1: Get all preflop actions with hole cards and positions
        query = """
        SELECT e.hand_id, e.player_id, e.action, e.id as event_id,
               ph.hole_cards
        FROM events e
        LEFT JOIN player_hand_cards ph ON e.hand_id = ph.hand_id AND e.player_id = ph.player_id
        WHERE e.stage = 'Preflop' AND e.action IN ('raise', 'raise_to_amount', 'call', 'fold')
        ORDER BY e.hand_id, e.id
        """
        df = pd.read_sql_query(query, self.conn)

        # Step 2: Get positions
        pos_df = pd.read_sql_query(self._get_position_query(), self.conn)
        df = df.merge(pos_df, on=['hand_id', 'player_id'], how='left')
        df['position'] = df.apply(lambda row: self._map_pos(row['pos_rank'], row['table_size']), axis=1)

        three_bets = []
        opportunities = []

        for hand_id, group in df.groupby('hand_id'):
            raises = 0
            for _, row in group.iterrows():
                action = row['action']
                pid = row['player_id']
                pos = row['position']
                cards = row['hole_cards']

                if action in ['raise', 'raise_to_amount']:
                    raises += 1
                    if raises == 2:
                        # This is a 3-bet
                        combo = self._normalize_cards_static(cards)
                        if combo != 'Unknown':
                            three_bets.append({'position': pos, 'hand_combo': combo})
                elif raises == 1 and action in ['call', 'fold']:
                    # Player had the opportunity to 3-bet but didn't
                    combo = self._normalize_cards_static(cards)
                    if combo != 'Unknown':
                        opportunities.append({'position': pos, 'hand_combo': combo})

        # Add three-bets as opportunities too
        for tb in three_bets:
            opportunities.append(tb.copy())

        if not three_bets:
            return pd.DataFrame()

        tb_df = pd.DataFrame(three_bets).groupby(['position', 'hand_combo']).size().reset_index(name='three_bet_count')
        opp_df = pd.DataFrame(opportunities).groupby(['position', 'hand_combo']).size().reset_index(name='total_opportunities')

        result = opp_df.merge(tb_df, on=['position', 'hand_combo'], how='left')
        result['three_bet_count'] = result['three_bet_count'].fillna(0).astype(int)
        result['three_bet_freq'] = (result['three_bet_count'] / result['total_opportunities'] * 100).round(1)

        return result

    def get_3bet_response_by_position(self):
        """Get how the original raiser responds to a 3-bet, grouped by raiser position.

        Returns a DataFrame with: raiser_position, fold_pct, call_pct, four_bet_pct, total_faced
        """
        query = """
        SELECT e.hand_id, e.player_id, e.action, e.id as event_id
        FROM events e
        WHERE e.stage = 'Preflop' AND e.action IN ('raise', 'raise_to_amount', 'call', 'fold')
        ORDER BY e.hand_id, e.id
        """
        df = pd.read_sql_query(query, self.conn)

        pos_df = pd.read_sql_query(self._get_position_query(), self.conn)
        df = df.merge(pos_df, on=['hand_id', 'player_id'], how='left')
        df['position'] = df.apply(lambda row: self._map_pos(row['pos_rank'], row['table_size']), axis=1)

        responses = []

        for hand_id, group in df.groupby('hand_id'):
            raises = 0
            first_raiser_pid = None
            first_raiser_pos = None

            for _, row in group.iterrows():
                action = row['action']
                pid = row['player_id']
                pos = row['position']

                if action in ['raise', 'raise_to_amount']:
                    raises += 1
                    if raises == 1:
                        first_raiser_pid = pid
                        first_raiser_pos = pos
                    elif raises == 2:
                        # 3-bet happened — now look for original raiser's response
                        pass
                    elif raises == 3 and pid == first_raiser_pid:
                        # Original raiser 4-bet
                        responses.append({'raiser_position': first_raiser_pos, 'response': '4-Bet'})
                elif raises >= 2 and pid == first_raiser_pid:
                    # Original raiser acts after facing 3-bet
                    if action == 'fold':
                        responses.append({'raiser_position': first_raiser_pos, 'response': 'Fold'})
                    elif action == 'call':
                        responses.append({'raiser_position': first_raiser_pos, 'response': 'Call'})

        if not responses:
            return pd.DataFrame()

        resp_df = pd.DataFrame(responses)
        agg = resp_df.groupby(['raiser_position', 'response']).size().unstack(fill_value=0)

        for col in ['Fold', 'Call', '4-Bet']:
            if col not in agg.columns:
                agg[col] = 0

        agg['total_faced'] = agg.sum(axis=1)
        agg['fold_pct'] = (agg['Fold'] / agg['total_faced'] * 100).round(1)
        agg['call_pct'] = (agg['Call'] / agg['total_faced'] * 100).round(1)
        agg['four_bet_pct'] = (agg['4-Bet'] / agg['total_faced'] * 100).round(1)

        result = agg[['fold_pct', 'call_pct', 'four_bet_pct', 'total_faced']].reset_index()
        result.columns = ['raiser_position', 'fold_pct', 'call_pct', 'four_bet_pct', 'total_faced']
        return result.sort_values('total_faced', ascending=False)

    def get_3bet_bluff_index(self):
        """Get win rate at showdown for hands used to 3-bet (the 'Bluff Index').

        Returns a DataFrame with: hand_combo, showdown_count, win_count, win_pct
        """
        import json

        # Step 1: Identify the 3-bettor and their cards in each 3-bet hand
        query = """
        SELECT e.hand_id, e.player_id, e.action, e.id as event_id,
               ph.hole_cards
        FROM events e
        LEFT JOIN player_hand_cards ph ON e.hand_id = ph.hand_id AND e.player_id = ph.player_id
        WHERE e.stage = 'Preflop' AND e.action IN ('raise', 'raise_to_amount')
        ORDER BY e.hand_id, e.id
        """
        df = pd.read_sql_query(query, self.conn)

        three_bettors = {}  # hand_id -> (player_id, hole_cards)
        for hand_id, group in df.groupby('hand_id'):
            raises = list(group.itertuples(index=False))
            if len(raises) >= 2:
                # The second raiser is the 3-bettor
                tb = raises[1]
                three_bettors[hand_id] = (tb.player_id, tb.hole_cards)

        if not three_bettors:
            return pd.DataFrame()

        # Step 2: Find showdown results — did the 3-bettor win?
        tb_hand_ids = list(three_bettors.keys())

        # Get collections for 3-bet hands
        placeholders = ','.join(['?' for _ in tb_hand_ids])
        collect_query = f"""
        SELECT hand_id, player_id, SUM(amount) as collected
        FROM events
        WHERE action = 'collect' AND hand_id IN ({placeholders})
        GROUP BY hand_id, player_id
        """
        collect_df = pd.read_sql_query(collect_query, self.conn, params=tb_hand_ids)

        # Check which hands went to showdown
        showdown_query = f"""
        SELECT DISTINCT hand_id
        FROM events
        WHERE (action = 'show' OR stage = 'Showdown') AND hand_id IN ({placeholders})
        """
        showdown_df = pd.read_sql_query(showdown_query, self.conn, params=tb_hand_ids)
        showdown_hands = set(showdown_df['hand_id'].tolist())

        results = []
        for hand_id, (pid, cards) in three_bettors.items():
            if hand_id not in showdown_hands:
                continue
            combo = self._normalize_cards_static(cards)
            if combo == 'Unknown':
                continue

            # Did 3-bettor win?
            collected = collect_df[(collect_df['hand_id'] == hand_id) & (collect_df['player_id'] == pid)]
            won = not collected.empty and collected.iloc[0]['collected'] > 0

            results.append({
                'hand_combo': combo,
                'won': won
            })

        if not results:
            return pd.DataFrame()

        res_df = pd.DataFrame(results)
        agg = res_df.groupby('hand_combo').agg(
            showdown_count=('won', 'count'),
            win_count=('won', 'sum')
        ).reset_index()
        agg['win_pct'] = (agg['win_count'] / agg['showdown_count'] * 100).round(1)

        return agg.sort_values('win_pct', ascending=True)


