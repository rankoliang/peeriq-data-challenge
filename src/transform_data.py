def good_standing(df):
    return df.filter(df["loan_status"] != "Charged Off")
