from src.featureengineering.extract_features import features_as_pandas_df


def train(df):
    pass

def main():
    df = features_as_pandas_df("data/sampledata.csv")
    df.head()
    print(df)
    train(df)

if __name__ == "__main__":
    main()
