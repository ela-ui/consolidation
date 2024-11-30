import pandas as pd
import streamlit as st
from io import BytesIO
import tempfile
import os

# Set Streamlit theme (light base, custom background color)
st.set_page_config(page_title="BRS Consolidation", page_icon="📊",layout="centered")

# Function to process the BRS reconciliation
def automate_brs_reconciliation(df1, df2):
    # Clean column names by stripping extra whitespace
    df1.columns = df1.columns.str.strip()

    # Rename columns in df1 for consistency
    df1.rename(columns={
        'remarks': 'UTR',
        'system_payment_date': 'Date',
        'collection_amount': 'Amount',
        'deposited_bank_name': 'Bank Name'
    }, inplace=True)

    # Convert 'Date' column in df1 to datetime and format it
    df1['Date'] = pd.to_datetime(df1['Date'], errors='coerce').dt.strftime('%d-%m-%Y')

    # Ensure UTR column is treated as a string
    df1['UTR'] = df1['UTR'].astype(str)

    # Remove duplicates by UTR and sum the Amount; keep first values for other columns
    df1_grouped = df1.groupby('UTR', as_index=False).agg({
        'Amount': 'sum',
        'Date': 'first',
        'Bank Name': 'first',
        'partner_name': 'first',
        'region': 'first',
        'hub_code': 'first',
        'hub_name': 'first',
        'spoke_code': 'first',
        'spoke_name': 'first',
        'MCC Centre Id': 'first',
        'MCC Centre Name': 'first',
        'RM/SO Id': 'first',
        'RM/SO Name': 'first',
        'State': 'first',
        'deposited_bank_account': 'first',
        'deposited_bank_branch': 'first',
        'bank_deposit_reference': 'first',
        'collected_by': 'first',
        'deposited_by': 'first',
        'account_number': 'first',
        'ClientID': 'first',
        'product_name': 'first',
        'product_code': 'first',
        'customer_name': 'first',
        'applicant_name': 'first',
        'customer id': 'first',
        'Applicant URN': 'first',
        'demand_date': 'first',
        'loan_amount': 'first',
        'schedule_demand_amount': 'first',
        'installment_number': 'first',
        'EMI Amount': 'first',
        'tenure': 'first',
        'instrument_type': 'first',
        'repayment_posted_date': 'first',
        'deposited_on_date': 'first',
        'principal_magnitude': 'first',
        'normal_interest_magnitude': 'first',
        'adjusted_security_emi': 'first',
        'fee_amount': 'first',
        'Penal_due': 'first',
        'Bounce_charges': 'first',
        'fee_waiver_amount': 'first',
        'Transaction Name': 'first',
        'status': 'first',
        'additional_interest_waiver_amount': 'first',
        'approved_by': 'first',
        'Approved Date and time': 'first',
        'stage': 'first',
        'Reject Reason': 'first',
        'Reject Remarks': 'first',
        'Rejected stage': 'first',
        'Rejected by': 'first',
        'BRS completed user': 'first',
        'BRS Completed Date': 'first',
        'Collection Checker Completed User': 'first',
        'Collection Checker Completed Date': 'first',
        'Assigned To': 'first',
        'Cash Collection remark': 'first',
        'RECEIPT_NO': 'first',
        'Product name': 'first',
        'MOP': 'first',
        'MRP': 'first',
        'cd_code': 'first',
        'category': 'first',
        'brand': 'first',
        'Applicant Address': 'first',
        'Applicant Mobile Number': 'first'
    })

    # Define Bank to Sheet Mapping
    bank_to_sheet_mapping = {
        'AirtelPayment': 'Airtel Payments Bank',
        'FinoBank': 'FinoBank',
        'SpiceMoney': 'Spice Money',
        'FingpayAccount': 'FingpayAccount',
        'SBIPowerJyothi': 'SBI PJ -7190',
        'Axis Bank': 'Axis Bank -4542'
    }

    # Define Column Mapping for Each Sheet
    sheet_column_mapping = {
        'Airtel Payments Bank': {'Transaction Id': 'UTR', 'Date and Time': 'Date', 'Original Input Amt': 'Amount', 'Bank Name': 'Bank Name'},
        'FinoBank': {'TRANSACTION ID': 'UTR', 'LOCAL DATE': 'Date', 'AMOUNT': 'Amount', 'Bank Name': 'Bank Name'},
        'Spice Money': {'Spice Txn ID': 'UTR', 'Date': 'Date', 'Amount': 'Amount', 'Bank Name': 'Bank Name'},
        'FingpayAccount': {'Fingpay Transaction Id': 'UTR', 'Corporate': 'Date', 'Drop Amount': 'Amount', 'Bank': 'Bank Name'},
        'SBI PJ -7190': {'Narration': 'UTR', 'Txn Date': 'Date', 'Credit': 'Amount', 'Bank Name': 'Bank Name'},
        'Axis Bank -4542': {'Transaction Particulars': 'UTR', 'Tran Date': 'Date', 'Amount(INR)': 'Amount', 'Bank Name': 'Bank Name'}
    }

    # Process each Bank Name in df1_grouped
    merged_results = []
    for bank_name, sheet_name in bank_to_sheet_mapping.items():
        if sheet_name in df2 and sheet_name in sheet_column_mapping:
            # Extract rows from df1_grouped for the current bank
            df1_filtered = df1_grouped[df1_grouped['Bank Name'] == bank_name]
            if df1_filtered.empty:
                continue

            # Get the corresponding sheet DataFrame
            df2_sheet = df2[sheet_name]

            # Rename columns based on the mapping
            column_mapping = sheet_column_mapping[sheet_name]
            missing_columns = [col for col in column_mapping.keys() if col not in df2_sheet.columns]
            if missing_columns:
                continue

            df2_sheet.rename(columns=column_mapping, inplace=True)
            df2_sheet['Date'] = pd.to_datetime(df2_sheet['Date'], errors='coerce').dt.strftime('%d-%m-%Y')
            df2_sheet['UTR'] = df2_sheet['UTR'].astype(str)
            df2_filtered = df2_sheet[df2_sheet['UTR'].isin(df1_filtered['UTR'])]

            # Merge DataFrames
            merged_df = df1_filtered.merge(df2_filtered[['UTR', 'Date', 'Amount', 'Bank Name']], on='UTR', how='left', suffixes=('', '_df2'))

            # Add status columns
            merged_df['date_status'] = merged_df.apply(
                lambda row: 'matched' if row['Date'] == row['Date_df2'] else 'mismatched', axis=1
            )
            merged_df['amount_status'] = merged_df.apply(
                lambda row: 'matched' if row['Amount'] == row['Amount_df2'] else 'mismatched', axis=1
            )
            merged_df['bank_name_status'] = merged_df.apply(
                lambda row: 'matched' if row['Bank Name'] == row['Bank Name_df2'] else 'mismatched', axis=1
            )
            merged_df['utr_status'] = merged_df.apply(
                lambda row: 'matched' if pd.notna(row['Date_df2']) else 'mismatched', axis=1
            )

            # Add final_status column
            merged_df['final_status'] = merged_df.apply(
                lambda row: 'Ok' if (
                    row['date_status'] == 'matched' and 
                    row['amount_status'] == 'matched' and 
                    row['utr_status'] == 'matched' and 
                    row['bank_name_status'] == 'matched'
                ) else 'Not Ok',
                axis=1
            )

            # Drop temporary columns used for comparison
            merged_df.drop(columns=['Date_df2', 'Amount_df2', 'Bank Name_df2'], inplace=True)
            merged_results.append(merged_df)

    # Concatenate and save the final DataFrame
    if merged_results:
        final_df = pd.concat(merged_results, ignore_index=True)
        
        # Use tempfile to create a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmpfile:
            output_file = tmpfile.name
            final_df.to_excel(output_file, index=False)  # Write with headers included
            return output_file
    else:
        return None

# Streamlit UI for file upload and processing
st.title("BRS DATA CONSOLIDATION")
uploaded_file1 = st.file_uploader("Upload the first file (BRS DATA.xlsx)", type=["xlsx"])
uploaded_file2 = st.file_uploader("Upload the second file (Bank Statement Data.xlsx)", type=["xlsx"])

if uploaded_file1 and uploaded_file2:
    # Read the uploaded files into pandas DataFrames
    df1 = pd.read_excel(uploaded_file1)
    df2 = pd.read_excel(uploaded_file2, sheet_name=None)

    # Reconcile and get the result
    result_file = automate_brs_reconciliation(df1, df2)
    
    if result_file:
        # Provide download link
        with open(result_file, "rb") as file:
            st.download_button(
                label="Download The validation File",
                data=file,
                file_name="validation_file.xlsx",
                mime="application/vnd.ms-excel"
            )
    else:
        st.write("No valid data to process. Please check your input files.")