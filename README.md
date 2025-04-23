# ecommerce-migration

## Overview

`data_export.py` is a Python script designed to facilitate the migration of data from Lightspeed systems to Shopify for an individual retail client. The script includes multiple functions that handle various aspects of the data export process, such as filtering inventory data, converting gift cards, and exporting customer, vendor, and transaction data. It processes data from CSV files, performs transformations, and generates new CSV files for import into Shopify.

## Features

The script provides the following functionalities:

### Inventory Management
- **`filter_inventory_data`**: Filters inventory data based on transaction dates and conditions.
- **Inventory Export (`export_inventory_data`)**: Prepares inventory data for Shopify, including mapping product categories, tags, and additional fields.

### Customer Data
- **Customer Export (`export_customer_data`)**: Processes customer information and formats it for Shopify, including handling email and SMS marketing preferences.

### Gift Card Management
- **`convert_credit_accounts_to_gift_cards`**: Converts credit account balances into gift card data.
- **`convert_gift_cards_to_rise_gift_cards`**: Processes active gift cards and prepares them for Shopify integration.

### Other Data Exports
- **Line Data (`export_line_data`)**: Copies transaction line data.
- **Purchase Order Data (`export_po_data`)**: Copies purchase order data.
- **Credit Account Data (`export_credit_account_data`)**: Copies credit account data.
- **Vendor Data (`export_vendor_data`)**: Copies vendor information.
- **Log Data (`export_log_data`)**: Copies inventory log details.

### Tag and Category Management
- **`get_category_dict`**: Reads and maps product categories from a CSV file.
- **`add_category_to_tags`**: Adds tags to products based on their categories.

## Usage

To execute the script, run the following command:

```bash
python data_export.py
```
### Data source
Necessary data files can be extracted from lightspeed, data templates can be retrieved from shopify, and the category mapping CSV must be made based on the needs of the user
