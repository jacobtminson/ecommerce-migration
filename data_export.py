import pandas as pd
import numpy as np
import os

def filter_inventory_data(filter=True):
    if not filter:
        return pd.read_csv('../../lightspeed-shopify-import-data/reports_inventory_listings_assets.csv')
    ldf = pd.read_csv('../../lightspeed-shopify-import-data/reports_inventory_listings_inventory_logs.csv')
    ldf_grouped = (ldf.sort_values('Date/Time', ascending=False)
                   .groupby('System ID')
                   .first()
                   .reset_index()[['System ID', 'Date/Time']]
                   .rename(columns={'Date/Time': 'Most Recent Transaction Date'}))

    idf = pd.read_csv('../../lightspeed-shopify-import-data/reports_inventory_listings_assets.csv', low_memory=False)
    total_product_count = idf.shape[0]

    merged = idf.merge(ldf_grouped, on='System ID', how='left')
    merged.to_csv('../../lightspeed-shopify-intermediate-data/most-recent-transactions.csv', index=False)

    condition = (
            (merged['Most Recent Transaction Date'] < '2022-01-01') |
            (merged['Most Recent Transaction Date'].isnull()) |
            ((merged['Most Recent Transaction Date'] < '2023-01-01') &
             (merged['Remaining'] == 0))
    )

    filtered_merged = merged[~condition]
    unfiltered_merged = merged[condition]

    unfiltered_merged.to_csv('../../lightspeed-shopify-intermediate-data/data_to_remove.csv', index=False)

    print(
        f"Filtered out {total_product_count - filtered_merged.shape[0]} products out of {total_product_count} total products")
    return filtered_merged

def get_category_dict():
    df = pd.read_csv('../../lightspeed-shopify-intermediate-data/categories - categories.csv', low_memory=False)
    #create a dictionary using column 1 as the key and column 2 as the value
    return dict(zip(df.iloc[:, 0], df.iloc[:, 1]))

def add_category_to_tags(df, category, tag_name_list):
    df['Tags'] = df['Tags'].fillna('').astype(str)
    df['Product category'] = df['Product category'].astype(str)
    for tag_name in tag_name_list:
        df['Tags'] = df.apply(lambda x: tag_name if x['Tags'] == '' and category in x['Product category'] else (x['Tags'] + ', ' + tag_name if category in x['Product category'] else x['Tags']), axis=1)
    return df

def convert_credit_accounts_to_gift_cards():
    df = pd.read_csv('../../lightspeed-shopify-import-data/credit_account_listings_credit_accounts.csv',
                     low_memory=False)
    df.Debt = df.Debt.str.replace('$', '').astype(float)
    df.Debt = abs(df.Debt)
    df['Credit Limit'] = df['Credit Limit'].str.replace('$', '').astype(float)
    df = df[df.Debt > 0]
    df['FullName'] = df['First Name'] + ' ' + df['Last Name']
    df['FullName'] = df['FullName'].str.replace('  ', ' ')
    df = df.rename(columns={'FullName': 'customer_name', 'Debt': 'balance', 'Email': 'customer_email'})
    out_df = df[['customer_name', 'balance', 'customer_email']]
    return out_df
    #out_df.to_csv('../../lightspeed-shopify-intermediate-data/credit_account_to_gift_card.csv', index=False)

def convert_gift_cards_to_rise_gift_cards():
    df = pd.read_csv('../../lightspeed-shopify-import-data/gift_card_export.csv', low_memory=False)
    df = df[df.Status == 'active']
    cdf = pd.read_csv('../../lightspeed-shopify-import-data/customer_listings_customers.csv', low_memory=False)
    df = df.merge(cdf[['Customer ID', 'First Name', 'Last Name']], left_on='Customer ID', right_on='Customer ID',
                  how='left')
    df['FullName'] = df['First Name'] + ' ' + df['Last Name']
    df = df[['Code', 'Balance', 'FullName']]
    df['FullName'] = df['FullName'].replace('Amy Boutique Account Giveaways/Promotions', np.nan)
    df['FullName'] = df['FullName'].replace('Employee  Birthday', np.nan)

    rdf = pd.read_csv('../../lightspeed-shopify-intermediate-data/rise_gift_card_template.csv', low_memory=False)
    cdf = convert_credit_accounts_to_gift_cards()

    rdf['code'] = df['Code']
    rdf['balance'] = df['Balance']
    rdf['customer_name'] = df['FullName']
    # add cdf to the bottom of rdf
    rdf = pd.concat([rdf, cdf])
    rdf.to_csv('../../shopify-data-out/rise_gift_card_output.csv', index=False)


def export_inventory_data():
    df = filter_inventory_data()
    sdf = pd.read_csv('../../lightspeed-shopify-intermediate-data/product_template.csv')
    # empty sdf
    sdf = sdf.drop(sdf.index)
    # map df 'Item' to sdf 'Title' and 'Category' to 'Product Category'
    sdf['Title'] = df['Item']
    #sdf['Product category'] = df['Category']


    sdf['Vendor'] = 'Amy Boutique'
    sdf['SKU'] = df['Custom SKU']
    sdf['Inventory quantity'] = df['Remaining']
    sdf['Price'] = df['Sale Price'].str.replace('$', '').astype(float)
    sdf['Cost per item'] = df['Avg. Cost'].str.replace('$', '').astype(float)
    sdf['Product category'] = df['Category']

    sdf = add_category_to_tags(sdf, 'DISPLAY', ['display'])
    sdf = add_category_to_tags(sdf, 'HOLIDAY / 4th of July', ['4thofjuly', 'holiday'])
    sdf = add_category_to_tags(sdf, 'HOLIDAY / Christmas', ['christmas', 'holiday'])
    sdf = add_category_to_tags(sdf, 'HOLIDAY / Halloween', ['halloween', 'holiday'])
    sdf = add_category_to_tags(sdf, 'HOLIDAY / Easter', ['easter', 'holiday'])
    sdf = add_category_to_tags(sdf, "HOLIDAY / St Patrick's", ['St. Patricks Day', 'holiday'])
    sdf = add_category_to_tags(sdf, 'HOLIDAY / Valentines', ['valentines', 'holiday'])
    sdf = add_category_to_tags(sdf, 'HOLIDAY / Thanksgiving', ['thanksgiving', 'holiday'])

    category_dict = get_category_dict()
    sdf['Product category'] = df['Category'].map(category_dict)
    sdf["Product category"] = sdf["Product category"].fillna("")
    sdf.to_csv('../../shopify-data-out/product_data_output.csv', index=False)

def export_customer_data():
    df = pd.read_csv('../../lightspeed-shopify-import-data/customer_listings_customers.csv')
    sdf = pd.read_csv('../../lightspeed-shopify-intermediate-data/customer_template.csv')
    # empty sdf
    sdf = sdf.drop(sdf.index)
    sdf['First Name'] = df['First Name']
    sdf['Last Name'] = df['Last Name']
    sdf['Email'] = df['Email']
    sdf['Accepts Email Marketing'] = df['No Email'].apply(lambda x: 'no' if x == 1 else 'yes')
    sdf['Default Address Company'] = df['Company']
    sdf['Default Address Address1'] = df['Address1']
    sdf['Default Address Address2'] = df['Address2']
    sdf['Default Address City'] = df['City']
    sdf['Default Address Province Code'] = df['State'].apply(lambda x: 'VA' if x == 'Virginia' else x)
    sdf['Default Address Country Code'] = df['Country'].apply(
        lambda x: 'US' if x == 'United States' else 'AU' if x == 'Australia' else x)
    sdf['Default Address Zip'] = df['ZIP']
    # pull phone data from Home column first if it exists, then Work if it doesn't exist, then Fax if that doesn't exist, then Pager
    sdf['Phone'] = df['Home'].fillna(df['Work']).fillna(df['Fax']).fillna(df['Pager'])
    sdf['Accepts SMS Marketing'] = df['No Phone'].apply(
        lambda x: 'no' if x == 1 else 'yes')  # had to use "No Phone" column because there is no "No SMS" column
    sdf['Note'] = df['Note']
    #todo: figure out if we need to try and add credit account data here as metafields
    sdf.to_csv('../../shopify-data-out/customer_data_output.csv', index=False)

def export_line_data():
    os.system('cp ../../lightspeed-shopify-import-data/reports_sales_listings_transaction_line.csv ../../shopify-data-out/line_data_output.csv')

def export_po_data():
    os.system('cp ../../lightspeed-shopify-import-data/purchase_listings_purchases.csv ../../shopify-data-out/po_data_output.csv')

def export_credit_account_data():
    os.system('cp ../../lightspeed-shopify-import-data/credit_account_listings_credit_accounts.csv ../../shopify-data-out/credit_account_data_output.csv')

def export_vendor_data():
    os.system('cp ../../lightspeed-shopify-import-data/vendor_listings_vendors.csv ../../shopify-data-out/vendor_data_output.csv')

def export_log_data():
    os.system('cp ../../lightspeed-shopify-import-data/reports_inventory_listings_inventory_logs.csv ../../shopify-data-out/log_data_output.csv')

def export_gift_card_data():
    convert_gift_cards_to_rise_gift_cards()
    os.system('cp ../../lightspeed-shopify-import-data/gift_card_export.csv ../../shopify-data-out/gift_card_data_output.csv')

if __name__ == "__main__":
    try:
        export_inventory_data()
        print("Inventory data exported")
    except Exception as e:
        print("ERROR", e)
    try:
        export_customer_data()
        print("Customer data exported")
    except Exception as e:
        print("ERROR", e)
    try:
        export_line_data()
        print("Line data exported")
    except Exception as e:
        print("ERROR", e)
    try:
        export_po_data()
        print("PO data exported")
    except Exception as e:
        print("ERROR", e)
    try:
        export_credit_account_data()
        print("Credit account data exported")
    except Exception as e:
        print("ERROR", e)
    try:
        export_vendor_data()
        print("Vendor data exported")
    except Exception as e:
        print("ERROR", e)
    try:
        export_log_data()
        print("Log data exported")
    except Exception as e:
        print("ERROR", e)
    try:
        export_gift_card_data()
        print("Gift card data exported")
    except Exception as e:
        print("ERROR", e)
