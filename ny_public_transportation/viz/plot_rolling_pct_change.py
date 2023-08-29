from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.express as px

from ..utils.rolling_pct_change import get_rolling_pct_change
#from ..utils.constants import APIKeys

def plot_rolling_pct_change(
        df, window: str = "7D", datetime_col: str = "datetime_rounded",
        cols: list = None, groupby_key: str = None,
        citibike_cols: list = ['start_count', 'end_count'],
        mta_cols: list = ['entries_diff', 'exits_diff'],
):
    """
    Plot the rolling percent change of a dataframe, only supports one station at a time.
    """
    # require both citibike_cols and mta_cols in the dataframe
    assert set(citibike_cols).issubset(df.columns), "citibike_cols must be a subset of df.columns."
    assert set(mta_cols).issubset(df.columns), "mta_cols must be a subset of df.columns."

    df = get_rolling_pct_change(df, window, datetime_col, cols, groupby_key)
    # TODO: Refactor this to be more general
    df = df.rename(
        columns={
            col : "citibike_" + col for col in citibike_cols
        }
    )
    df = df.rename(
        columns={
            col : "subway_" + col for col in mta_cols
        }
    )
    citibike_cols = ["citibike_" + col for col in citibike_cols]
    mta_cols = ["subway_" + col for col in mta_cols]
    is_start_or_end = lambda s: 'start' if s in [citibike_cols[0], mta_cols[0]] else 'end'


    df_plot = (df
                .melt(id_vars=datetime_col, value_vars=df.columns[1:], var_name='type', value_name='pct_change')
                .assign(transportation = lambda df: df['type'].str.split('_').str[0]) # citibike or subway
                .assign(type = lambda df: df['type'].apply(is_start_or_end)) # start or end
            )
    figure = make_subplots(rows=2, cols=1, 
                           subplot_titles=['Detailed',
                                           'Difference, citibike% - subway%'],
                           shared_xaxes=True,
    )
    figure.update_layout(
        title=f"Rolling Percentage Change of {window}, Ridership",
    )

    # plotly express does not support explicit subplot generation, so we have to unpack the traces
    unpack_express_to_trace = (
        lambda px_fig, parent_fig, row, col:
            px_fig.for_each_trace(lambda t: parent_fig.add_trace(t, row=row, col=col))
    )
    unpack_express_to_trace(
        px.line(
            data_frame=df_plot,
            x=datetime_col,
            y='pct_change',
            color='transportation',
            line_dash='type',
            labels={
                datetime_col: 'Date',
                'pct_change': 'Percentage Change',
                'transportation': 'Transportation',
                'type': 'Start or End'
            },
        ), figure, 1, 1
    )

    unpack_express_to_trace(
        px.line(
            data_frame=(
                df_plot
                .groupby([datetime_col, 'type'])
                .apply(
                    lambda window: window.assign(
                        pct_change_diff = lambda df: 
                            df.loc[df['transportation'] == 'citibike', 'pct_change'].values[0] -
                            df.loc[df['transportation'] == 'subway', 'pct_change'].values[0]
                    )
                )
                #.sort_values(['datetime', 'type', 'transportation'], ascending=[True, False, True])
                .drop(['transportation', 'pct_change'], axis=1)
                .drop_duplicates()
                .reset_index(drop=True)
            ),
            x=datetime_col,
            y='pct_change_diff',
            color='type',
            color_discrete_sequence=['#1f77b4', '#ff7f0e'],
            symbol='type',
            symbol_sequence=['circle', 'circle'],
            labels={
                datetime_col: 'Date',
                'pct_change_diff': 'citibike% - subway%',
                'type': 'Start or End'
            },
            #title=f"Rolling Percentage Change of {kwargs['window']}, 
        ), figure, 2, 1
    )

    # add striaght lines at 0
    for i in range(1, 3):
        figure.add_trace(
            go.Scatter(
                x=df_plot[datetime_col].unique(),
                y=[0] * len(df_plot[datetime_col].unique()),
                mode='lines',
                line=dict(color='black', width=1, dash='dash'),
                showlegend=False,
                name='0% Line'
            ), row=i, col=1
        )
    figure.for_each_yaxis(lambda a: a.update(tickformat='.2%'))
    figure.for_each_annotation(lambda a: a.update(font=dict(size=14)))
    figure.update_layout(
        template='simple_white',
    )

    return figure