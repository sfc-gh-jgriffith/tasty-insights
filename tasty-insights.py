# Import python packages
import streamlit as st
import datetime
import pandas
from snowflake.snowpark.context import get_active_session

import snowflake.snowpark.functions as f

# Get the current credentials
session = get_active_session()
orders = session.table("ORDERS_V")

def get_data_insights(prompt, data):
    completion_model = 'llama2-70b-chat'

    prompt_prefix = """
        You are an assistant helping derive insights from a dataset containing order information from a global 
        network of food trucks. I will provide you with a question and a dataset, and you will respond
        in a concise way that answers the question. Provide answers based only on the provided data. 
        Respond with a playful tone, incorporating food puns into the body of the response. Limit your response to 500 words.

    """

    if type(data) == pandas.core.frame.DataFrame:
        data_string = data.to_string(index=False)
    else:
        data_string = data.to_pandas().to_string(index=False)

    prompt = f"""select snowflake.cortex.complete(
                                '{completion_model}', 
                                $$
                                    {prompt_prefix}
                                    {prompt}
                                    ###
                                    {data_string}
                                    ###
                                $$) as INSIGHT
             """

    return session.sql(prompt).collect()[0]["INSIGHT"]

st.title("Tasty Bytes Insights :balloon:")

analysis_dimensions = ['Truck Brand Name', 'Region', 'Country', 'Gender', 'Marital Status']
selected_dimension = st.selectbox("Analysis Dimension", analysis_dimensions)
selected_dimension_colname = str(selected_dimension).upper().replace(' ','_')


# date ranges available in data
@st.cache_data
def get_date_range():
    return orders.select(f.min("DATE").alias("MIN_DATE"), 
                               f.max("DATE").alias("MAX_DATE")).collect()[0]

@st.cache_data
def get_brands():
    brands = orders.select(f.col("TRUCK_BRAND_NAME")).distinct().collect()
    return [r["TRUCK_BRAND_NAME"] for r in brands]

with st.sidebar:
    dates_selection = st.slider("Date Range",get_date_range()["MIN_DATE"], get_date_range()["MAX_DATE"],
                 value=(datetime.date(2021,11, 1), datetime.date(2022,10,31)))
    
    start_date = dates_selection[0].strftime('%Y-%m-%d')
    end_date = dates_selection[1].strftime('%Y-%m-%d')

    selected_brands = st.multiselect("Select Brands", get_brands(), default=get_brands())

# base dataframe that incorparates our user filters
analysis_base = orders.filter( (f.col("DATE") >= start_date) & 
                               (f.col("DATE") <= end_date) & 
                               (f.col("TRUCK_BRAND_NAME").isin(selected_brands) )
                             ).with_column(selected_dimension_colname, 
                                           f.coalesce(selected_dimension_colname, f.lit("NULL")))

# order totals
orders_by_analysis_dimension = analysis_base.group_by(selected_dimension_colname).count()

st.header(f"_Sales by {selected_dimension}_")
st.bar_chart(orders_by_analysis_dimension, x=selected_dimension_colname, y="COUNT")

bar_chart_insights = st.button("Get Insights", key="bar_chart_insights_button")
if bar_chart_insights:
    with st.expander(f"{selected_dimension_colname} Insights", expanded=True):
    
        resp = get_data_insights(f"Here are sales by {selected_dimension_colname}. What are some insights from this data?", 
                                 orders_by_analysis_dimension)
        st.write(resp)
        
# order totals by day
st.header(f"_Sales by {selected_dimension} by Day_")
# get list of the values for our dimension column for use in the pivotfunction
dimension_values = [r[selected_dimension_colname] for r in analysis_base.select(selected_dimension_colname).distinct().collect()]
orders_by_dimension_by_day = analysis_base.select([selected_dimension_colname, "DATE", f.lit(1).alias("CT")])\
                                            .pivot(
                                                f.col(selected_dimension_colname),
                                                dimension_values
                                            ).sum(f.col("CT")).sort(f.col("DATE"), ascending=False)

# deal with quotes from the pivot function
# note, column names get converted to UPPER CASE
for b in zip(dimension_values, orders_by_dimension_by_day.columns[1:]) :
    orders_by_dimension_by_day = orders_by_dimension_by_day.with_column_renamed(f.col(b[1]), b[0].upper())

orders_by_dimension_by_day = orders_by_dimension_by_day.with_column("Day of Week", f.call_function("decode"
                                                                                                        , f.dayofweek(f.col("DATE"))
                                                                                                        , 0, "Sunday"
                                                                                                        , 1, "Monday"
                                                                                                        , 2, "Tuesday"
                                                                                                        , 3, "Wednesday"
                                                                                                        , 4, "Thursday"
                                                                                                        , 5, "Friday"
                                                                                                        , 6, "Saturday")
                                                                       )
st.line_chart(orders_by_dimension_by_day , x="DATE", y=[v.upper() for v in dimension_values])

daily_insights = st.button("Get Insights", key="daily_insights_button")
if daily_insights:
    with st.expander(f"{selected_dimension} Daily Insights", expanded=True):
        
        resp = get_data_insights(f"""Here are sales by {selected_dimension} by day. What {selected_dimension}s are trending upward?
                                    Are there any days of the week where {selected_dimension}s overperform or underperform?""", 
                                 orders_by_dimension_by_day.limit(15))
        st.write("Insights limited to the last 15 days to limit prompt size")
        st.write(resp)
