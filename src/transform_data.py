def goodStanding(df):
    return df.filter(df["loan_status"] != "Charged Off")
