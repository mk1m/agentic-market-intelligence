import sqlite3
import joblib
import ollama
import pandas as pd

def get_latest_context():
    '''
    Fetches the latest data row and generates an XGBoost prediction.
    '''
    # connect to DB
    conn = sqlite3.connect("data/market_data.db")
    query = "SELECT * FROM btc_metrics ORDER BY Date DESC LIMIT 1"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        return None

    # load trained XGBoost model
    model = joblib.load("models/xgboost_forecaster.pkl")
    
    # features for prediction
    features = ["Lag_1", "Lag_2", "Lag_3", "Volatility"]
    X_latest = df[features]
    
    prediction_return = model.predict(X_latest)[0]
    
    # return dictionary of all data points
    context = df.iloc[0].to_dict()
    context['predicted_return'] = prediction_return
    return context

def run_agentic_analysis(ctx):
    '''
    Feeds the statistical prediction + real data into the LLM.
    '''
    # if these columns are missing, set to N/A
    ma7 = ctx.get('MA7', 'N/A')
    ma21 = ctx.get('MA21', 'N/A')
    volatility = ctx.get('Volatility', 'N/A')
    
    # Calculations for simple trend descriptions
    trend = "going up" if ctx['MA7'] > ctx['MA21'] else "going down"
    
    prompt = f"""
    [PERSONA]
    You are a friendly, patient Financial Mentor. Your goal is to explain Bitcoin 
    trends to someone who has never invested before. Avoid jargon. If you use a 
    technical term, explain it with an everyday analogy.

    [DATA INPUTS]
    - Current Price: ${ctx['Close']:.2f}
    - 7-Day Average (Short-term mood): ${ma7:.2f}
    - 21-Day Average (Long-term mood): ${ma21:.2f}
    - Volatility: {volatility:.2%}
    - Math Model Prediction: {ctx['predicted_return']:.2%} change expected tomorrow.

    [INSTRUCTIONS]
    1. Start with a "Bottom Line" summary (e.g., "The mood is positive").
    2. Explain the "Moving Averages" as a "Battle between recent mood and past mood."
    3. Explain the XGBoost prediction as a "Calculated guess based on history."
    4. Provide a clear 'Action' with a 'Why' sentence that references the data.
    """
    
    response = ollama.chat(model='llama3', messages=[
        {'role': 'system', 'content': 'You are a helpful mentor for beginner investors. Use simple English and relatable analogies.'},
        {'role': 'user', 'content': prompt},
    ])
    
    return response['message']['content']

if __name__ == "__main__":
    context_data = get_latest_context()
    if context_data:
        print("Agent is thinking...")
        analysis = run_agentic_analysis(context_data)
        print("\n" + "="*30)
        print("FINAL AGENT REPORT")
        print("="*30)
        print(analysis)
    else:
        print("No data found in database. Run the pipeline first.")