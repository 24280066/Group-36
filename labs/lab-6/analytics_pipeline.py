from prefect import task, flow, get_run_logger
import pandas as pd
import matplotlib.pyplot as plt

@task
def fetch_data():
    logger = get_run_logger()
    logger.info("Reading data...")
    df = pd.read_csv("analytics_data.csv")
    logger.info(f"Data shape: {df.shape}")
    return df

@task
def validate_data(df):
    logger = get_run_logger()
    logger.info("Validating data...")
    missing_values = df.isnull().sum()
    logger.info(f"Missing values:\n{missing_values}")
    df_clean = df.dropna()
    return df_clean

@task
def transform_data(df):
    logger = get_run_logger()
    logger.info("Transforming data...")
    if "sales" in df.columns:
        df["sales_normalized"] = (df["sales"] - df["sales"].mean()) / df["sales"].std()
    return df

@task
def generate_report(df):
    logger = get_run_logger()
    logger.info("Generating analytics report...")
    summary = df.describe()
    summary.to_csv("analytics_summary.csv")
    logger.info("Saved summary to analytics_summary.csv")

@task
def plot_histogram(df):
    logger = get_run_logger()
    if "sales" in df.columns:
        plt.hist(df["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig("sales_histogram.png")
        plt.close()
        logger.info("Sales histogram saved to sales_histogram.png")

@flow
def analytics_flow():
    df = fetch_data()
    clean_df = validate_data(df)
    transformed = transform_data(clean_df)
    generate_report(transformed)
    plot_histogram(transformed)

if __name__ == "__main__":
    analytics_flow()
