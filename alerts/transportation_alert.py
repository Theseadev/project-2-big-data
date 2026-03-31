def generate_alert(df):
    alerts = []
    if len(df) > 100:
        alerts.append("🚨 High traffic volume")
    
    if df["fare"].max() > 90000:
        alerts.append("💰 High fare detected")
        
    return alerts