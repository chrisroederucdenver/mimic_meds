import os
import re
import pandas as pd

def import_mimic_iv_text(
    mimic_iv_discharge_df: pd.DataFrame,
    label_dir_path: str,
    label_filename: str,
) -> pd.DataFrame:

    """
    Imports MIMIC-IV-Note data into a “Text” column of the label dataframe.
        Parameters:
            mimic_iv_discharge_df (pd.DataFrame): DataFrame containing discharge
                summaries from MIMIC-IV-Note discharge.csv.gz.
            label_dir_path (str): The directory path of label CSV files.
            label_filename (str): The filename of a label CSV file.
        Returns:
            pd.DataFrame: The DataFrame with labels and associated MIMIC-IV text.
    """

    note_id = extract_note_id(label_filename)
    document_row = mimic_iv_discharge_df.loc[mimic_iv_discharge_df.note_id == note_id]
    document_text = document_row.text.iloc[0]
    label_df = pd.read_csv(os.path.join(label_dir_path, label_filename))
    label_df["Text"] = None

    # Mapping each label to its corresponding text
    for idx, row in label_df.iterrows():
        ##text_slice = slice(row["Start Position"], row["End Position"])
        text_slice = slice(row["Start Position"]-1, row["End Position"])
        word = document_text[text_slice]
        label_df.at[idx, "Text"] = word.strip() 

    return label_df


def extract_note_id(s: str) -> str:
    """
    Extracts the note id from a string containing 'NoteID-' followed by the actual note id.
        Parameters:
            s (str): The string to extract the note id from.
        Returns:
            str: The extracted note id.
        Raises:
            ValueError: If the note id cannot be extracted.
    """

    match = re.search(r"NoteID-([0-9A-Za-z-]+)", s)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Could not extract note_id from {s}")


if __name__ == '__main__':
    df = pd.read_csv('/Users/roederc/work/git_gen_ai/resources/mimic-iv-note/discharge.csv')
    folder=('/Users/roederc/work/git_gen_ai/resources/mimic-iv-note-med_labels/'
            'medication-extraction-labels-for-mimic-iv-note-clinical-database-1.0.0/'
            'mimic-iv-note-labels')
    label_df = None
    for thing in os.listdir(folder):
        filepath = os.path.join(folder, thing)
        if os.path.isfile(filepath):
            new_label_df = import_mimic_iv_text(df, folder, thing)
            if label_df is None:
                label_df = new_label_df
            else:
                label_df = pd.concat([label_df, new_label_df], ignore_index=True)

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', None) 
    print(label_df)



