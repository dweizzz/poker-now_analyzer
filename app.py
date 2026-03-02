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
view_mode = st.sidebar.radio("Select View", ["Exploit Dashboard", "Player Profile", "Net PnL Leaderboard"])

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

                # Verification String
                if not tb.empty:
                    most_common_texture = tb['texture_tags'].mode().iloc[0] if not tb['texture_tags'].mode().empty else 'Dry'
                    most_common_size = tb['sizing_bucket'].mode().iloc[0] if not tb['sizing_bucket'].mode().empty else 'Unknown'
                    st.info(f"**Verification Check:** In this pool, players triple-barreling on **{most_common_texture}** boards with **{most_common_size}** bets show up with Air **{freq:.1f}%** of the time.")

                # Full Lines Table
                st.divider()
                st.markdown("### All Action Lines (Showdown Analyzed)")
                showdowns = df_lines.dropna(subset=['strength_tier'])
                if not showdowns.empty:
                    agg = showdowns.groupby('action_line').agg(
                        total_showdowns=('strength_tier', 'count'),
                        bluffs=('strength_tier', lambda x: (x <= 1).sum()),
                        wins=('strength_tier', lambda x: (x >= 2).sum())
                    ).reset_index()
                    agg['bluff_freq'] = (agg['bluffs'] / agg['total_showdowns']) * 100
                    agg['wsd_pct'] = (agg['wins'] / agg['total_showdowns']) * 100
                    agg = agg.sort_values('total_showdowns', ascending=False)

                    st.dataframe(
                        agg.style.format({'bluff_freq':'{:.1f}%', 'wsd_pct':'{:.1f}%'})
                                 .background_gradient(subset=["bluff_freq"], cmap="Reds")
                                 .background_gradient(subset=["wsd_pct"], cmap="Greens"),
                        use_container_width=True, hide_index=True
                    )
                else:
                    st.write("No showdown data available for lines.")

                # Example Lines Engine
                st.markdown("### Example Lines Inspector")
                st.write("View individual instances of an action line to study the exact boards and hand strengths.")
                unique_lines = df_lines['action_line'].unique().tolist()
                selected_line = st.selectbox("Select Action Line:", sorted(unique_lines))

                if selected_line:
                    examples = df_lines[(df_lines['action_line'] == selected_line) & (df_lines['strength_tier'].notna())]
                    if not examples.empty:
                        # Map strength tier back to text for readability
                        tier_map = {0: 'Air (High Card)', 1: 'Weak (Pair)', 2: 'Medium (Top/Two Pair)', 3: 'Strong (Set/Str/Fl)', 4: 'Nuts (FH+)'}
                        examples['Hand Strength'] = examples['strength_tier'].map(tier_map)

                        display_examples = examples[['hand_id', 'display_name', 'position', 'sizing_bucket', 'texture_tags', 'Hand Strength']]
                        display_examples.columns = ['Hand ID', 'Player', 'Position', 'Final Sizing', 'Board Textures', 'Hand Strength']
                        st.dataframe(display_examples, use_container_width=True, hide_index=True)

                        st.markdown("#### Full Hand Log")
                        selected_hand = st.selectbox("Select Hand to inspect:", examples['hand_id'].unique())
                        if selected_hand:
                            events_df = engine.get_hand_events(selected_hand)
                            if not events_df.empty:
                                if 'position' in events_df.columns:
                                    display_log = events_df[['stage', 'position', 'actor', 'action', 'amount', 'pot_size', 'board_cards', 'details']]
                                    display_log.columns = ['Stage', 'Pos', 'Player', 'Action', 'Amount', 'Pot', 'Board', 'Details']
                                else:
                                    display_log = events_df[['stage', 'actor', 'action', 'amount', 'pot_size', 'board_cards', 'details']]
                                    display_log.columns = ['Stage', 'Player', 'Action', 'Amount', 'Pot', 'Board', 'Details']

                                # Estimate height to eliminate vertical scrollbar (approx 35px per row + 40px header)
                                optimal_height = len(display_log) * 35 + 40
                                st.dataframe(display_log, use_container_width=True, hide_index=True, height=optimal_height)

                    else:
                        st.info("No showdown examples recorded for this line yet.")

            else:
                st.info("No action line data available.")

    else:
        st.info("No sufficient data for Visual Analytics (requires >100 hands per player).")

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
