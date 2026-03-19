def identify_and_remove_duplicated_data(df, subset=None, inplace=False):
    """
    Identifies and removes duplicated rows from the DataFrame.

    Parameters:
    - df: the input DataFrame
    - subset: list of column names to consider for duplicate detection (default: all columns)
    - inplace: if True, modifies the original DataFrame (default: False)

    Returns:
    - A cleaned DataFrame with duplicates removed
    """
    duplicate_count = df.duplicated(subset=subset).sum()

    if duplicate_count > 0:
        print("-" * 50)
        print(f"Found {duplicate_count} duplicate rows")
        print("Shape before:", df.shape)

        if inplace:
            df.drop_duplicates(subset=subset, keep='first', inplace=True)
            print("Shape after:", df.shape)
            print("-" * 50)
            return df
        else:
            df_cleaned = df.drop_duplicates(subset=subset, keep='first')
            print("Shape after:", df_cleaned.shape)
            print("-" * 50)
            return df_cleaned
    else:
        print("✅ No duplicate rows found")
        return df if inplace else df.copy()
