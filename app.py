import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import subprocess
import os
from analytics import PokerAnalytics

st.set_page_config(page_title="PokerNow Analytics MVP", layout="wide")

# Auto-ingestion logic
@st.cache_resource
def run_ingestion():
    input_dir = 'data/to_be_ingested'
    # Ensure directory exists just in case
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs('data/ingested', exist_ok=True)

    # Check if there are any json files to ingest
    has_files = any(f.endswith('.json') for f in os.listdir(input_dir)) if os.path.exists(input_dir) else False

    if has_files:
        with st.spinner("New logs detected. Ingesting to database..."):
            subprocess.run(["python3", "ingest.py", input_dir])

            from analytics import PokerAnalytics
            try:
                PokerAnalytics('pokernow.db').calculate_and_store_player_priors()
            except Exception as e:
                print("Error calculating priors:", e)

    return has_files

# Run it
new_data_processed = run_ingestion()

if new_data_processed:
    st.cache_resource.clear()
    # Need to rerun to clear the caching states properly for other functions
    st.rerun()

st.title("PokerNow Analytics Dashboard")

def get_analytics():
    return PokerAnalytics('pokernow.db')

analytics = get_analytics()

priors_df = analytics.get_priors()
if not priors_df.empty:
    priors_df = priors_df[priors_df['total_hands'] >= 50]

st.sidebar.header("Navigation")

show_leaks = os.environ.get("SHOW_LEAKS", "").lower() in ("true", "1", "yes")
try:
    if "SHOW_LEAKS" in st.secrets and st.secrets["SHOW_LEAKS"]:
        show_leaks = True
except Exception:
    pass

views = ["Exploit Dashboard", "3-Bet Analysis", "Line Inspector", "Player Profile", "Net PnL Leaderboard"]
if show_leaks:
    views.append("My Leaks (Dan)")

if 'view_mode' not in st.session_state:
    st.session_state.view_mode = views[0]

view_mode_index = views.index(st.session_state.view_mode) if st.session_state.view_mode in views else 0

st.sidebar.radio("Select View", views, index=view_mode_index, key='view_mode_radio', on_change=lambda: st.session_state.update(view_mode=st.session_state.view_mode_radio))

view_mode = st.session_state.view_mode

if view_mode == "Exploit Dashboard":
    st.header("Opponent Intelligence & Exploit Dashboard")

    st.info("""
    **Statistic Definitions:**
    * **WTSD% (Went to Showdown):** Percentage of times a player goes to showdown after seeing the flop. (Optimal ~25-30%)
    * **WSD% (Won Money at Showdown):** Percentage of times a player wins the pot when they go to showdown. (Optimal >50%)
    * **WWSF% (Won When Saw Flop):** Percentage of hands won after seeing the flop. (Optimal ~45-50%)
    * **River Bluff%:** Percentage of times a player bet or raised on the river, got called, and lost the hand.
    """)

    st.subheader("Target List")
    targets_df = analytics.get_exploit_targets()
    if not targets_df.empty:
        display_targets = targets_df[['display_name', 'player_id', 'total_hands', 'wtsd_pct', 'wsd_pct', 'wwsf_pct', 'river_bluff_freq', 'profile_tag']].rename(columns={
            'display_name': 'Name',
            'player_id': 'Player ID',
            'total_hands': 'Hands',
            'wtsd_pct': 'WTSD%',
            'wsd_pct': 'WSD%',
            'wwsf_pct': 'WWSF%',
            'river_bluff_freq': 'River Bluff%',
            'profile_tag': 'Profile Tag'
        })
        st.dataframe(
            display_targets.style.format({
                "WTSD%": "{:.1f}%",
                "WSD%": "{:.1f}%",
                "WWSF%": "{:.1f}%",
                "River Bluff%": "{:.1f}%"
            }).background_gradient(subset=["WTSD%", "WWSF%"], cmap="Oranges"),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No sufficient data for Target List (requires >= 50 hands per player).")

    st.subheader("Visual Analytics")
    visual_df = analytics.get_visual_analytics_data(min_hands=100)

    if not visual_df.empty:
        st.markdown("### The Regulars Scatter (>100 Hands)")
        fig1, ax1 = plt.subplots(figsize=(10, 7))

        # Use 'RdYlGn' to color by win rate. Green for positive, Red for negative.
        scatter1 = sns.scatterplot(
            data=visual_df,
            x='vpip_pct',
            y='pfr_pct',
            hue='bb_per_100',
            palette='RdYlGn',
            size='total_hands',
            sizes=(100, 800),
            alpha=0.8,
            ax=ax1,
            legend=True
        )

        for _, row in visual_df.iterrows():
            ax1.annotate(
                row['display_name'],
                (row['vpip_pct'], row['pfr_pct']),
                xytext=(0, 8),
                textcoords='offset points',
                ha='center',
                fontsize=9
            )

        ax1.set_xlabel("VPIP %")
        ax1.set_ylabel("PFR %")
        ax1.set_title("VPIP vs PFR (colored by BB/100)")
        ax1.set_xlim(left=0)
        ax1.set_ylim(bottom=0)

        # Place legend outside so it doesn't overlap points
        # We want to keep the 'bb_per_100' legend but maybe suppress the 'total_hands' size legend to keep it clean
        handles, labels = ax1.get_legend_handles_labels()
        # Find the index where sizes start to split out the hue vs size legends
        try:
            size_index = labels.index('total_hands')
            ax1.legend(handles[:size_index], labels[:size_index], title="BB/100", bbox_to_anchor=(1.05, 1), loc='upper left')
        except ValueError:
            ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

        st.pyplot(fig1)

        st.divider()

        st.markdown("### Showdown Efficiency")
        fig2, ax2 = plt.subplots(figsize=(10, 7))

        sns.scatterplot(
            data=visual_df,
            x='wtsd_pct',
            y='wsd_pct',
            hue='profile_tag',
            size='total_hands',
            sizes=(100, 800),
            alpha=0.8,
            ax=ax2
        )

        for _, row in visual_df.iterrows():
            ax2.annotate(
                row['display_name'],
                (row['wtsd_pct'], row['wsd_pct']),
                xytext=(0, 8),
                textcoords='offset points',
                ha='center',
                fontsize=9
            )

        # Vertical line at 27% average WTSD
        ax2.axvline(27, color='red', linestyle='--', label='Avg WTSD (27%)')
        # Add a horizontal line at 50% WSD indicating break-even at showdown
        ax2.axhline(50, color='gray', linestyle=':', label='Break-even WSD (50%)')

        ax2.set_xlabel("WTSD %")
        ax2.set_ylabel("WSD %")
        ax2.set_title("WTSD vs WSD")
        ax2.set_xlim(left=0)
        ax2.set_ylim(bottom=0)

        handles, labels = ax2.get_legend_handles_labels()
        try:
            size_index = labels.index('total_hands')
            ax2.legend(handles[:size_index], labels[:size_index], title="Profile Tag", bbox_to_anchor=(1.05, 1), loc='upper left')
        except ValueError:
            ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left')

        st.pyplot(fig2)

        st.divider()
        st.subheader("Line Exploit Engine")

        st.info("""
        **Action Line Terminology:**
        * **PFR:** Preflop Raise
        * **PFC:** Preflop Call
        * **F-Bet / F-Call:** Flop Bet / Flop Call
        * **T-Bet / T-Call:** Turn Bet / Turn Call
        * **R-Bet / R-Call:** River Bet / River Call
        _Example: `PFC_F-Call_T-Bet_R-Bet` means the player called preflop, called the flop, bet the turn, and bet the river._
        """)

        with st.spinner("Analyzing Action Lines..."):
            from analytics import LineExploitEngine
            engine = LineExploitEngine('pokernow.db')
            df_lines = engine.build_action_lines()

            if not df_lines.empty:
                tb, freq, sizing_groups = engine.get_triple_barrel_auditor(df_lines)
                ub, ob = engine.get_exploit_finder(df_lines)
                tex_map = engine.get_texture_bluff_map(df_lines)

                # Sizing Heatmap
                st.markdown("### Sizing vs. Strength Correlation")
                if not sizing_groups.empty:
                    ordered = ['Overbet (>120%)', 'Large (80-120%)', 'Medium (40-80%)', 'Small (<40%)']
                    actual_ordered = [o for o in ordered if o in sizing_groups.index]
                    if actual_ordered:
                        sizing_groups = sizing_groups.reindex(actual_ordered)
                    fig_sz, ax_sz = plt.subplots(figsize=(8, 4))
                    sns.heatmap(sizing_groups, annot=True, fmt=".0f", cmap="Reds", ax=ax_sz)
                    ax_sz.set_xlabel("Strength Tier (0=Air, 4=Nuts)")
                    ax_sz.set_ylabel("Sizing Bucket")
                    st.pyplot(fig_sz)
                else:
                    st.info("No sizing correlation data available.")

                # Texture Bluff Map
                st.markdown("### Texture Bluff Map (Air at Showdown %)")
                if not tex_map.empty:
                    fig_tx, ax_tx = plt.subplots(figsize=(8, 4))
                    sns.barplot(data=tex_map, x='tag', y='air_pct', hue='tag', palette='viridis', legend=False, ax=ax_tx)
                    ax_tx.set_ylabel("Air % at Showdown")
                    ax_tx.set_xlabel("Board Texture Tag")
                    for container in ax_tx.containers:
                        ax_tx.bar_label(container, fmt='%.1f%%')
                    st.pyplot(fig_tx)

                # Top 3 Exploit Lines
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("#### Top 3 Under-Bluffed Lines (<10% Bluff)")
                    if not ub.empty:
                        st.dataframe(ub[['action_line', 'total_showdowns', 'bluff_freq', 'wsd_pct']].style.format({'bluff_freq':'{:.1f}%', 'wsd_pct':'{:.1f}%'}), hide_index=True)
                    else:
                        st.write("None found.")
                with col2:
                    st.markdown("#### Top 3 Over-Bluffed Lines (>40% Bluff)")
                    if not ob.empty:
                        st.dataframe(ob[['action_line', 'total_showdowns', 'bluff_freq', 'wsd_pct']].style.format({'bluff_freq':'{:.1f}%', 'wsd_pct':'{:.1f}%'}), hide_index=True)
                    else:
                        st.write("None found.")



            else:
                st.info("No action line data available.")

    else:
        st.info("No sufficient data for Visual Analytics (requires >100 hands per player).")

elif view_mode == "Line Inspector":
    st.header("Action Lines Inspector")
    st.info("Study detailed data on all player action lines and jump into specific example hands.")

    with st.spinner("Analyzing Action Lines..."):
        from analytics import LineExploitEngine
        engine = LineExploitEngine('pokernow.db')
        df_lines = engine.build_action_lines()

        if not df_lines.empty:
            st.markdown("### All Action Lines")
            agg = df_lines.groupby('action_line').agg(
                total_hands=('hand_id', 'count'),
                total_showdowns=('strength_tier', lambda x: x.notna().sum()),
                bluffs=('strength_tier', lambda x: (x <= 1).sum()),
                wins=('strength_tier', lambda x: (x >= 2).sum())
            ).reset_index()
            agg['bluff_freq'] = agg.apply(lambda r: (r['bluffs'] / r['total_showdowns']) * 100 if r['total_showdowns'] > 0 else float('nan'), axis=1)
            agg['wsd_pct'] = agg.apply(lambda r: (r['wins'] / r['total_showdowns']) * 100 if r['total_showdowns'] > 0 else float('nan'), axis=1)
            agg = agg.sort_values('total_hands', ascending=False)

            st.dataframe(
                agg.style.format({'bluff_freq':'{:.1f}%', 'wsd_pct':'{:.1f}%', 'total_hands': '{:.0f}', 'total_showdowns': '{:.0f}'})
                         .background_gradient(subset=["bluff_freq"], cmap="Reds")
                         .background_gradient(subset=["wsd_pct"], cmap="Greens"),
                use_container_width=True, hide_index=True
            )

            # If jumping from another tab (e.g. 3-Bet Analysis), show the targeted hand immediately
            if 'target_hand_id' in st.session_state and st.session_state.target_hand_id:
                target_hand = st.session_state.target_hand_id
                del st.session_state.target_hand_id
                st.markdown(f"### 🎯 Targeted Hand: `{target_hand}`")
                events_df = engine.get_hand_events(target_hand)
                if not events_df.empty:
                    if 'position' in events_df.columns:
                        display_log = events_df[['stage', 'position', 'actor', 'action', 'amount', 'pot_size', 'board_cards', 'details']]
                        display_log.columns = ['Stage', 'Pos', 'Player', 'Action', 'Amount', 'Pot', 'Board', 'Details']
                    else:
                        display_log = events_df[['stage', 'actor', 'action', 'amount', 'pot_size', 'board_cards', 'details']]
                        display_log.columns = ['Stage', 'Player', 'Action', 'Amount', 'Pot', 'Board', 'Details']
                    optimal_height = len(display_log) * 35 + 40
                    st.dataframe(display_log, use_container_width=True, hide_index=True, height=optimal_height)
                else:
                    st.warning(f"No events found for hand {target_hand}.")
                st.divider()

            st.markdown("### Example Lines Inspector")
            st.write("View individual instances of an action line to study the exact boards and hand strengths.")
            unique_lines = df_lines['action_line'].unique().tolist()

            selected_line = st.selectbox("Select Action Line:", sorted(unique_lines))

            if selected_line:
                examples = df_lines[df_lines['action_line'] == selected_line]
                if not examples.empty:
                    tier_map = {0: 'Air (High Card)', 1: 'Weak (Pair)', 2: 'Medium (Top/Two Pair)', 3: 'Strong (Set/Str/Fl)', 4: 'Nuts (FH+)'}
                    examples_copy = examples.copy()
                    examples_copy['Hand Strength'] = examples_copy['strength_tier'].map(tier_map).fillna('No Showdown')

                    display_examples = examples_copy[['hand_id', 'display_name', 'position', 'sizing_bucket', 'texture_tags', 'Hand Strength']]
                    display_examples.columns = ['Hand ID', 'Player', 'Position', 'Final Sizing', 'Board Textures', 'Hand Strength']
                    st.dataframe(display_examples, use_container_width=True, hide_index=True)
                else:
                    st.info("No examples recorded for this line yet.")

            st.markdown("#### Full Hand Log")

            all_hands = df_lines['hand_id'].unique().tolist()
            selected_hand = st.selectbox("Select Hand to inspect:", all_hands, key='inspect_hand_select')
            if selected_hand:
                events_df = engine.get_hand_events(selected_hand)
                if not events_df.empty:
                    if 'position' in events_df.columns:
                        display_log = events_df[['stage', 'position', 'actor', 'action', 'amount', 'pot_size', 'board_cards', 'details']]
                        display_log.columns = ['Stage', 'Pos', 'Player', 'Action', 'Amount', 'Pot', 'Board', 'Details']
                    else:
                        display_log = events_df[['stage', 'actor', 'action', 'amount', 'pot_size', 'board_cards', 'details']]
                        display_log.columns = ['Stage', 'Player', 'Action', 'Amount', 'Pot', 'Board', 'Details']

                    optimal_height = len(display_log) * 35 + 40
                    st.dataframe(display_log, use_container_width=True, hide_index=True, height=optimal_height)
        else:
            st.info("No action line data available.")

elif view_mode == "3-Bet Analysis":
    st.header("3-Bet Analysis Dashboard")


    with st.spinner("Analyzing 3-Bet Action..."):
        from analytics import LineExploitEngine
        engine = LineExploitEngine('pokernow.db')
        df_lines = engine.build_action_lines()

        if not df_lines.empty:
            st.markdown("### 3-Bet Pots vs Single Raised Pots (SRP)")
            stats_3b = engine.get_3bet_vs_srp_stats(df_lines)

            col_3b1, col_3b2, col_3b3 = st.columns(3)

            with col_3b1:
                st.markdown("#### Bluff Frequencies")
                st.metric("SRP Bluff %", f"{stats_3b['srp_bluff_pct']:.1f}%", help=f"Based on {stats_3b['srp_count']} showdowns")
                st.metric("3-Bet Pot Bluff %", f"{stats_3b['tb_bluff_pct']:.1f}%", help=f"Based on {stats_3b['tb_count']} showdowns")

            with col_3b2:
                st.markdown("#### 3-Bet Pot Post-Flop Raises")
                st.write("When a player raises post-flop in a 3-bet pot:")
                st.metric("Nuts / Premium %", f"{stats_3b['tb_pf_raise_strong_pct']:.1f}%", help=f"Based on {stats_3b['tb_pf_raise_count']} instances")
                st.metric("Air / Bluff %", f"{stats_3b['tb_pf_raise_air_pct']:.1f}%")

            with col_3b3:
                st.markdown("#### 3-Bettor Success")
                st.metric("3-Bettor Win Rate", f"{stats_3b['tb_win_pct']:.1f}%", help=f"Percentage of time the 3-bettor wins the pot (out of {stats_3b['tb_total_hands']} hands)")

            st.divider()
            st.markdown("### Preflop 3-Bet Calling Ranges")
            pf_stats = engine.get_3bet_preflop_stats()
            if not pf_stats.empty:
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("#### Calling Ranges")
                    cr = pf_stats.groupby('hand_combo').size().reset_index(name='count').sort_values('count', ascending=False)
                    st.dataframe(cr, use_container_width=True, hide_index=True)

                with col2:
                    st.markdown("#### By Position")
                    pr = pf_stats.groupby('position').size().reset_index(name='count').sort_values('count', ascending=False)
                    st.dataframe(pr, use_container_width=True, hide_index=True)

                st.markdown("#### Cold Callers (Only Action = Call 3bet)")
                cold = pf_stats[pf_stats['is_cold_call'] == True]
                if not cold.empty:
                    cold_df = cold[['display_name', 'position', 'hand_combo']].rename(columns={'display_name':'Player', 'position':'Pos', 'hand_combo':'Hand'})
                    st.dataframe(cold_df, use_container_width=True, hide_index=True)
                else:
                    st.write("No preflop cold calls of 3-bets found.")
            else:
                 st.info("No preflop 3-bets found.")

            st.divider()
            st.markdown("### Postflop Sizings in 3-Bet Pots")
            post_sizings = engine.get_3bet_postflop_sizings(df_lines)
            if not post_sizings.empty:
                col_sz1, col_sz2 = st.columns(2)
                with col_sz1:
                    st.markdown("#### Overall Postflop Sizings")
                    sz_agg = post_sizings.groupby('sizing_bucket').size().reset_index(name='count').sort_values('count', ascending=False)
                    st.dataframe(sz_agg, use_container_width=True, hide_index=True)
                with col_sz2:
                     st.markdown("#### Sizings by Street")
                     sz_st = post_sizings.groupby(['stage', 'sizing_bucket']).size().unstack(fill_value=0)
                     st.dataframe(sz_st, use_container_width=True)

                st.markdown("#### Inspect Specific Postflop 3-Bet Sizing")
                unique_sz = post_sizings['sizing_bucket'].unique().tolist()
                selected_sz = st.selectbox("Select Sizing to Inspect:", sorted(unique_sz), key='3b_sz_select')
                if selected_sz:
                     sz_examples = post_sizings[post_sizings['sizing_bucket'] == selected_sz]
                     st.dataframe(sz_examples[['hand_id', 'display_name', 'stage', 'action', 'amount', 'pot_size']], use_container_width=True, hide_index=True)

                     st.markdown("##### Full Hand Log")
                     sel_h = st.selectbox("Select Hand to inspect:", sz_examples['hand_id'].unique(), key='3b_hand_select')
                     if sel_h:
                         def _go_to_inspector(hand_id):
                             st.session_state.target_hand_id = hand_id
                             st.session_state.view_mode = "Line Inspector"
                             st.session_state.view_mode_radio = "Line Inspector"

                         st.button("Inspect Hand in Line Inspector", type="primary",
                                   on_click=_go_to_inspector, args=(sel_h,))
            else:
                st.info("No postflop actions in 3-bet pots found.")

            # ---- Visual Analysis Section ----
            st.divider()
            st.markdown("### 📊 Positional 3-Bet Frequency Trellis")
            st.caption("Each panel shows how often each hand combo is 3-bet from that position. Brighter = higher frequency.")

            trellis_data = engine.get_3bet_frequency_by_position_and_hand()
            if not trellis_data.empty:
                ranks = ['A', 'K', 'Q', 'J', 'T', '9', '8', '7', '6', '5', '4', '3', '2']
                positions = ['BTN', 'CO', 'HJ', 'SB', 'BB']
                available_positions = [p for p in positions if p in trellis_data['position'].unique()]

                if available_positions:
                    vmax_val = max(trellis_data['three_bet_freq'].max(), 1)

                    for pos in available_positions:
                        pos_data = trellis_data[trellis_data['position'] == pos]

                        # Build 13x13 matrix
                        freq_matrix = pd.DataFrame(np.nan, index=ranks, columns=ranks)
                        annot_matrix = pd.DataFrame("", index=ranks, columns=ranks)

                        for _, row in pos_data.iterrows():
                            combo = row['hand_combo']
                            freq = row['three_bet_freq']
                            if len(combo) == 2:  # Pocket pair
                                r1 = combo[0]
                                if r1 in ranks:
                                    freq_matrix.loc[r1, r1] = freq
                                    annot_matrix.loc[r1, r1] = f"{combo}\n{freq:.0f}%"
                            elif len(combo) == 3:
                                r1, r2, suit_type = combo[0], combo[1], combo[2]
                                if r1 in ranks and r2 in ranks:
                                    if suit_type == 's':
                                        freq_matrix.loc[r1, r2] = freq
                                        annot_matrix.loc[r1, r2] = f"{combo}\n{freq:.0f}%"
                                    else:
                                        freq_matrix.loc[r2, r1] = freq
                                        annot_matrix.loc[r2, r1] = f"{combo}\n{freq:.0f}%"

                        # Fill empty annotations with combo names
                        for r in ranks:
                            for c in ranks:
                                if annot_matrix.loc[r, c] == "":
                                    if r == c:
                                        annot_matrix.loc[r, c] = f"{r}{c}"
                                    elif ranks.index(r) < ranks.index(c):
                                        annot_matrix.loc[r, c] = f"{r}{c}s"
                                    else:
                                        annot_matrix.loc[r, c] = f"{c}{r}o"

                        fig_tr, ax = plt.subplots(figsize=(12, 10))
                        sns.heatmap(freq_matrix.astype(float), annot=annot_matrix, fmt="", cmap="YlOrRd",
                                    vmin=0, vmax=vmax_val,
                                    cbar=True, ax=ax, linewidths=0.3, linecolor='#ddd',
                                    annot_kws={"size": 8}, square=True)
                        ax.set_title(f"{pos}", fontsize=16, fontweight='bold')
                        ax.xaxis.tick_top()
                        ax.xaxis.set_label_position('top')
                        ax.tick_params(axis='both', labelsize=9)
                        plt.tight_layout()
                        st.pyplot(fig_tr)
                else:
                    st.info("No positional 3-bet data available for primary positions.")
            else:
                st.info("No 3-bet frequency data available.")

            st.divider()
            st.markdown("### 📊 3-Bet Response by Raiser Position")
            st.caption("When the original raiser faces a 3-bet, how do they respond? Segmented by raiser position.")

            response_data = engine.get_3bet_response_by_position()
            if not response_data.empty:
                fig_resp, ax_resp = plt.subplots(figsize=(14, 8))
                positions_order = ['BTN', 'CO', 'HJ', 'SB', 'BB', 'UTG', 'MP']
                resp_sorted = response_data.copy()
                resp_sorted['pos_order'] = resp_sorted['raiser_position'].apply(
                    lambda x: positions_order.index(x) if x in positions_order else 99)
                resp_sorted = resp_sorted.sort_values('pos_order')

                x_labels = resp_sorted['raiser_position'].tolist()
                x = np.arange(len(x_labels))
                width = 0.6

                fold_vals = resp_sorted['fold_pct'].values
                call_vals = resp_sorted['call_pct'].values
                fourbet_vals = resp_sorted['four_bet_pct'].values

                bars_fold = ax_resp.bar(x, fold_vals, width, label='Fold', color='#e74c3c', alpha=0.9)
                bars_call = ax_resp.bar(x, call_vals, width, bottom=fold_vals, label='Call', color='#3498db', alpha=0.9)
                bars_4bet = ax_resp.bar(x, fourbet_vals, width, bottom=fold_vals + call_vals, label='4-Bet', color='#2ecc71', alpha=0.9)

                # Add labels on segments
                for i in range(len(x_labels)):
                    total = resp_sorted.iloc[i]['total_faced']
                    if fold_vals[i] > 8:
                        ax_resp.text(x[i], fold_vals[i] / 2, f"{fold_vals[i]:.0f}%", ha='center', va='center', fontsize=8, color='white', fontweight='bold')
                    if call_vals[i] > 8:
                        ax_resp.text(x[i], fold_vals[i] + call_vals[i] / 2, f"{call_vals[i]:.0f}%", ha='center', va='center', fontsize=8, color='white', fontweight='bold')
                    if fourbet_vals[i] > 8:
                        ax_resp.text(x[i], fold_vals[i] + call_vals[i] + fourbet_vals[i] / 2, f"{fourbet_vals[i]:.0f}%", ha='center', va='center', fontsize=8, color='white', fontweight='bold')
                    ax_resp.text(x[i], 102, f"n={int(total)}", ha='center', va='bottom', fontsize=7, color='gray')

                ax_resp.set_xticks(x)
                ax_resp.set_xticklabels(x_labels)
                ax_resp.set_ylabel("Percentage")
                ax_resp.set_title("Original Raiser's Response to 3-Bet")
                ax_resp.legend(loc='upper right')
                ax_resp.set_ylim(0, 115)

                st.pyplot(fig_resp)
            else:
                st.info("No 3-bet response data available.")

            st.divider()
            st.markdown("### 🔴 Bluff Index Map (3-Bet Showdown Win Rate)")
            st.caption("Win rate at showdown for hands used to 3-bet. Red = losing (over-bluffed spots). Green = winning (value hands).")

            bluff_index_data = engine.get_3bet_bluff_index()
            if not bluff_index_data.empty:
                ranks = ['A', 'K', 'Q', 'J', 'T', '9', '8', '7', '6', '5', '4', '3', '2']
                win_matrix = pd.DataFrame(np.nan, index=ranks, columns=ranks)
                annot_matrix_bi = pd.DataFrame("", index=ranks, columns=ranks)

                for _, row in bluff_index_data.iterrows():
                    combo = row['hand_combo']
                    win_pct = row['win_pct']
                    count = int(row['showdown_count'])
                    if len(combo) == 2:
                        r1 = combo[0]
                        if r1 in ranks:
                            win_matrix.loc[r1, r1] = win_pct
                            annot_matrix_bi.loc[r1, r1] = f"{combo}\n{win_pct:.0f}% ({count})"
                    elif len(combo) == 3:
                        r1, r2, suit_type = combo[0], combo[1], combo[2]
                        if r1 in ranks and r2 in ranks:
                            if suit_type == 's':
                                win_matrix.loc[r1, r2] = win_pct
                                annot_matrix_bi.loc[r1, r2] = f"{combo}\n{win_pct:.0f}% ({count})"
                            else:
                                win_matrix.loc[r2, r1] = win_pct
                                annot_matrix_bi.loc[r2, r1] = f"{combo}\n{win_pct:.0f}% ({count})"

                # Fill empty annotations
                for r in ranks:
                    for c in ranks:
                        if annot_matrix_bi.loc[r, c] == "":
                            if r == c:
                                annot_matrix_bi.loc[r, c] = f"{r}{c}"
                            elif ranks.index(r) < ranks.index(c):
                                annot_matrix_bi.loc[r, c] = f"{r}{c}s"
                            else:
                                annot_matrix_bi.loc[r, c] = f"{c}{r}o"

                fig_bi, ax_bi = plt.subplots(figsize=(16, 14))
                sns.heatmap(win_matrix.astype(float), annot=annot_matrix_bi, fmt="", cmap="RdYlGn",
                            vmin=0, vmax=100, center=50,
                            cbar_kws={'label': 'Win Rate at Showdown (%)'}, ax=ax_bi,
                            linewidths=0.5, linecolor='gray', annot_kws={"size": 7}, square=True)
                ax_bi.xaxis.tick_top()
                ax_bi.xaxis.set_label_position('top')
                ax_bi.set_title("3-Bet Bluff Index: Win Rate at Showdown by Hand", pad=20, fontsize=14, fontweight='bold')
                st.pyplot(fig_bi)

                # Red Zone callout
                red_zones = bluff_index_data[bluff_index_data['showdown_count'] >= 3].head(5)
                if not red_zones.empty:
                    st.markdown("#### 🚨 Red Zone Hands (Lowest WSD% with ≥3 Showdowns)")
                    st.caption("These hands are being 3-bet but consistently lose at showdown — potential over-bluff spots.")
                    rz_display = red_zones[['hand_combo', 'showdown_count', 'win_count', 'win_pct']].rename(columns={
                        'hand_combo': 'Hand',
                        'showdown_count': 'Showdowns',
                        'win_count': 'Wins',
                        'win_pct': 'Win %'
                    })
                    st.dataframe(
                        rz_display.style.format({"Win %": "{:.1f}%"})
                            .background_gradient(subset=["Win %"], cmap="RdYlGn", vmin=0, vmax=100),
                        use_container_width=True, hide_index=True
                    )
            else:
                st.info("No bluff index data available.")

        else:
             st.info("No action line data available.")

elif view_mode == "Net PnL Leaderboard":
    st.header("Net PnL Leaderboard")
    pnl_df = analytics.get_net_pnl_all_players()
    if not pnl_df.empty:
        # Clean dataframe for display
        display_df = pnl_df.rename(columns={
            'player_id': 'Player ID',
            'display_name': 'Name',
            'total_net_pnl': 'Net PnL'
        })
        st.dataframe(
            display_df.style.format({"Net PnL": "{:.2f}"})
                            .background_gradient(subset=["Net PnL"], cmap="RdYlGn"),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No PnL data available.")

elif view_mode == "Player Profile":
    st.sidebar.header("Player Search")
    if not priors_df.empty:
        # Create a mapping dictionary of formatted name -> player_id
        player_options = {}
        for _, row in priors_df.iterrows():
            display_label = f"{row['display_name']} ({row['player_id']})"
            player_options[display_label] = row['player_id']

        selected_label = st.sidebar.selectbox("Select a Player", list(player_options.keys()))

        if selected_label:
            selected_player_id = player_options[selected_label]
            # We can extract just the display name for the headers
            display_name = selected_label.rsplit(" (", 1)[0]
            st.header(f"Player Profile: {display_name}")

            # 1. Priors Section
            st.subheader("Priors (Preflop Statistics)")
        player_priors = priors_df[priors_df['player_id'] == selected_player_id].iloc[0]

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Hands", int(player_priors['total_hands']))
        col2.metric("VPIP %", f"{player_priors['vpip_pct']}%")
        col3.metric("PFR %", f"{player_priors['pfr_pct']}%")
        col4.metric("3-Bet %", f"{player_priors['three_bet_pct']}%")

        st.divider()

        # 2. Bet Sizing Frequencies
        st.subheader("Post-Flop Bet-Sizing Frequencies")
        bet_sizing_df = analytics.get_bet_sizing_frequencies()
        if not bet_sizing_df.empty and selected_player_id in bet_sizing_df.index:
            player_bets = bet_sizing_df.loc[[selected_player_id]]
            st.bar_chart(player_bets.T)
        else:
            st.info("No post-flop bets recorded for this player.")

        st.divider()

        # 3. Positional Heatmap
        st.subheader(f"Positional Profit/Loss Heatmap for {display_name}")
        pl_df = analytics.get_profit_loss_by_position(selected_player_id)

        if not pl_df.empty:
            # We want to display this as a heatmap. So we format it into a 1-row pivot
            pl_df = pl_df.set_index('position').T

            fig, ax = plt.subplots(figsize=(8, 2))
            sns.heatmap(pl_df, annot=True, cmap="RdYlGn", center=0, cbar=True, ax=ax, fmt=".1f")
            ax.set_yticklabels(["Net Profit"], rotation=0)
            ax.set_xlabel("Table Position")
            st.pyplot(fig)
        else:
            st.info("No positional profit/loss data available for this player.")

        st.divider()

        # 4. Positional Stats (VPIP, PFR, 3-Bet)
        st.subheader("Positional Preflop Statistics")
        pos_stats_df = analytics.get_positional_stats(selected_player_id)
        if not pos_stats_df.empty:
            display_pos_df = pos_stats_df[['position', 'total_hands', 'vpip_pct', 'pfr_pct', 'three_bet_pct']].rename(columns={
                'position': 'Position',
                'total_hands': 'Hands',
                'vpip_pct': 'VPIP %',
                'pfr_pct': 'PFR %',
                'three_bet_pct': '3-Bet %'
            })
            st.dataframe(
                display_pos_df.style.format({
                    "VPIP %": "{:.1f}%",
                    "PFR %": "{:.1f}%",
                    "3-Bet %": "{:.1f}%"
                }).background_gradient(subset=["VPIP %", "PFR %", "3-Bet %"], cmap="Blues"),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No positional preflop statistics available.")

        st.divider()

        # 5. Hand PNL (Preflop Chart)
        st.subheader("Profit/Loss by Hole Cards (Preflop Chart)")
        hand_pnl_df = analytics.get_pnl_by_hand(selected_player_id)
        if not hand_pnl_df.empty:
            ranks = ['A', 'K', 'Q', 'J', 'T', '9', '8', '7', '6', '5', '4', '3', '2']
            pnl_matrix = pd.DataFrame(index=ranks, columns=ranks, data=np.nan)

            # Populate matrix
            for _, row in hand_pnl_df.iterrows():
                combo = row['hand_combo']
                pnl = row['total_pnl']
                if combo == 'Unknown' or len(combo) not in [2, 3]:
                    continue
                r1, r2 = combo[0], combo[1]
                if len(combo) == 2:
                    pnl_matrix.loc[r1, r2] = pnl
                elif combo[2] == 's':
                    pnl_matrix.loc[r1, r2] = pnl
                elif combo[2] == 'o':
                    pnl_matrix.loc[r2, r1] = pnl

            # Generate annotations
            annot_matrix = pd.DataFrame(index=ranks, columns=ranks, data="")
            for r in ranks:
                for c in ranks:
                    val = pnl_matrix.loc[r, c]
                    combo_name = ""
                    if r == c: combo_name = f"{r}{c}"
                    elif ranks.index(r) < ranks.index(c): combo_name = f"{r}{c}s"
                    else: combo_name = f"{c}{r}o"

                    if pd.isna(val):
                        annot_matrix.loc[r, c] = combo_name
                    else:
                        annot_matrix.loc[r, c] = f"{combo_name}\n{val:.0f}"

            fig, ax = plt.subplots(figsize=(10, 10))
            sns.heatmap(pnl_matrix, annot=annot_matrix, fmt="", cmap="RdYlGn", center=0,
                        cbar_kws={'label': 'Net PnL'}, ax=ax, linewidths=0.5, linecolor='gray',
                        annot_kws={"size": 8})

            # Move x-axis labels to top
            ax.xaxis.tick_top()
            ax.xaxis.set_label_position('top')
            ax.set_aspect('equal')
            st.pyplot(fig)

            # Optional: Still show top/bottom 5 as a small table below
            st.write("---")
            display_hand_df = hand_pnl_df.rename(columns={
                'hand_combo': 'Hand',
                'times_dealt': 'Times Dealt',
                'total_pnl': 'Net PnL'
            })
            display_hand_df = display_hand_df[display_hand_df['Hand'] != 'Unknown']
            col_a, col_b = st.columns(2)
            with col_a:
                st.write("**Top 5 Most Profitable**")
                st.dataframe(
                    display_hand_df.head(5).style.format({"Net PnL": "{:.2f}"}).background_gradient(subset=["Net PnL"], cmap="RdYlGn"),
                    use_container_width=True,
                    hide_index=True
                )
            with col_b:
                st.write("**Bottom 5 Most Profitable**")
                st.dataframe(
                    display_hand_df.tail(5).sort_values('Net PnL', ascending=True).style.format({"Net PnL": "{:.2f}"}).background_gradient(subset=["Net PnL"], cmap="RdYlGn"),
                    use_container_width=True,
                    hide_index=True
                )
        else:
            st.info("No hole card PNL data available.")

    else:
        st.warning("No data found in database. Please run ingest.py first.")

elif view_mode == "My Leaks (Dan)":
    st.header("My Leaks: Most & Least Profitable Lines")
    st.info("Analyze your (Dan) specific action lines to see where you are making or losing the most money.")

    with st.spinner("Analyzing Action Lines..."):
        from analytics import LineExploitEngine
        engine = LineExploitEngine('pokernow.db')
        df_lines = engine.build_action_lines()

        if not df_lines.empty:
            hero_leaks_df = engine.get_hero_leaks(df_lines, hero_id="EJd9KHwjJa")

            if not hero_leaks_df.empty:
                # Filter for significance
                sig_leaks = hero_leaks_df[hero_leaks_df['occurrences'] >= 5]
                if sig_leaks.empty:
                    sig_leaks = hero_leaks_df

                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("### Most Profitable Lines")
                    top_lines = sig_leaks.head(10).copy()
                    st.dataframe(
                        top_lines.style.format({'total_pnl': '{:.2f}', 'avg_pnl': '{:.2f}'})
                            .background_gradient(subset=["total_pnl"], cmap="Greens"),
                        use_container_width=True, hide_index=True
                    )

                with col2:
                    st.markdown("### Least Profitable Lines")
                    bottom_lines = sig_leaks.tail(10).sort_values('total_pnl', ascending=True).copy()
                    st.dataframe(
                        bottom_lines.style.format({'total_pnl': '{:.2f}', 'avg_pnl': '{:.2f}'})
                            .background_gradient(subset=["total_pnl"], cmap="Reds"),
                        use_container_width=True, hide_index=True
                    )

                st.divider()
                st.markdown("### All Hero Action Lines")
                st.dataframe(
                    hero_leaks_df.style.format({'total_pnl': '{:.2f}', 'avg_pnl': '{:.2f}'})
                        .background_gradient(subset=["total_pnl"], cmap="RdYlGn"),
                    use_container_width=True, hide_index=True
                )

                st.markdown("### Example Lines Inspector (Hero)")
                st.write("View individual instances of your action lines to study the exact boards and hand strengths.")
                unique_lines = hero_leaks_df['action_line'].unique().tolist()
                selected_line = st.selectbox("Select Action Line:", sorted(unique_lines), key="hero_line_select")

                if selected_line:
                    examples = df_lines[(df_lines['action_line'] == selected_line) & (df_lines['player_id'] == "EJd9KHwjJa") & (df_lines['strength_tier'].notna())]
                    if not examples.empty:
                        tier_map = {0: 'Air (High Card)', 1: 'Weak (Pair)', 2: 'Medium (Top/Two Pair)', 3: 'Strong (Set/Str/Fl)', 4: 'Nuts (FH+)'}
                        examples_copy = examples.copy()
                        examples_copy['Hand Strength'] = examples_copy['strength_tier'].map(tier_map)

                        display_examples = examples_copy[['hand_id', 'position', 'sizing_bucket', 'texture_tags', 'Hand Strength']]
                        display_examples.columns = ['Hand ID', 'Position', 'Final Sizing', 'Board Textures', 'Hand Strength']
                        st.dataframe(display_examples, use_container_width=True, hide_index=True)

                        st.markdown("#### Full Hand Log")
                        selected_hand = st.selectbox("Select Hand to inspect:", examples['hand_id'].unique(), key="hero_hand_select")
                        if selected_hand:
                            events_df = engine.get_hand_events(selected_hand)
                            if not events_df.empty:
                                if 'position' in events_df.columns:
                                    display_log = events_df[['stage', 'position', 'actor', 'action', 'amount', 'pot_size', 'board_cards', 'details']]
                                    display_log.columns = ['Stage', 'Pos', 'Player', 'Action', 'Amount', 'Pot', 'Board', 'Details']
                                else:
                                    display_log = events_df[['stage', 'actor', 'action', 'amount', 'pot_size', 'board_cards', 'details']]
                                    display_log.columns = ['Stage', 'Player', 'Action', 'Amount', 'Pot', 'Board', 'Details']

                                optimal_height = len(display_log) * 35 + 40
                                st.dataframe(display_log, use_container_width=True, hide_index=True, height=optimal_height)
                    else:
                         st.info("No showdown examples recorded for this line yet.")
            else:
                st.info("No action line data available for Hero.")
        else:
            st.info("No action line data available.")

