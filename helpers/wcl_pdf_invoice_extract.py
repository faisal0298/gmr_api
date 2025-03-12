import re
from PyPDF2 import PdfReader
import PyPDF3

 
def extract_with_regex_wcl_rail(pattern, text, group_index=1):
    match = re.search(pattern, text,re.MULTILINE)
    if match:
        return match.group(group_index).strip()
    return None  # or return a default value, e.g., "Not Found"
 
def extract_invoice_data_wcl_rail(text):
    text = re.sub(r'\s{2,}', ' ', text)  # Replace multiple spaces/newlines with single space

    data = {}
    key_mapping = {
        'basic_price': 'basic_price',
        'sizing_charges': 'sizing_charges',
        'stc_charges': 'stc_charges',
        'evac_facility_charge': 'evac_facility_charge',
        'gst_comp_cess': 'gst_comp_cess',
        'gross_bill_value': 'gross_bill_value',
        'less_underloading_charges': 'te)',
        'net_value': 'net_value',
    }

    # Improved regex to match rows
    pattern = r'([A-Za-z\s\(\)%\-]+?)\s+([\d,\.]*)\s+([\d,\.]+)'
    matches = re.findall(pattern, text)

    for match in matches:
        description, rate, amount = match

        # Normalize description
        description_key = description.lower().strip().replace(" ", "_")

        # Match description to key
        for keyword, keywords_list in key_mapping.items():
            if isinstance(keywords_list, str):
                keywords_list = [keywords_list]
            if any(k in description_key for k in keywords_list):
                # Parse values
                rate_value = float(rate.replace(",", "")) if rate else 0.0
                amount_value = float(amount.replace(",", ""))
                data[f'{keyword}_rate'] = rate_value
                data[f'{keyword}_amount'] = amount_value
                break


    charge_pattern = r'(Royalty|NMET|DMF|CGST|SGST)(?:[^0-9]*?\([^)]*\))?\s*([0-9,.]+)'
    
    # Find all matches in text
    matches = re.finditer(charge_pattern, text, re.IGNORECASE)
    
    for match in matches:
        charge_type = match.group(1).lower()  # Convert to lowercase for consistency
        amount = float(match.group(2).replace(",", ""))
        
        data[f'{charge_type}_rate'] = 0.0
        data[f'{charge_type}_amount'] = amount

    return data

def extract_values_wcl_rail(text):
    item_pattern = r'''
        (\d+)\s+                    # Del No.
        ([A-Za-z\s]+OC\s+Mine)\s+   # Plant
        (\d+)\s+                    # Material
        (G\d+/-\d+\s*MM)\s+         # Grade/Size
        (\d+-\d+)\s+                # GCV
        ([^2]+?)\s+                 # Material Description
        (\d+)\s+                    # HSN Code
        (\w+)\s+                    # UOM
        ([\d.]+)\s+                 # STC Charges
        ([\d.]+)\s+                 # Basic Rate
        ([\d.]+)                    # Billed Quantity
    '''
    item_match = re.search(item_pattern, text, re.VERBOSE | re.DOTALL)    
    if item_match:
        return {
            'del_no': item_match.group(1), #Del No
            'plant': item_match.group(2).replace('\n', '').strip(), #Plant
            'material': item_match.group(3).strip(), #Material
            'grade_size': item_match.group(4).strip(), #Grade/Size
            'gcv': item_match.group(5).strip(), #GCV
            'material_description': item_match.group(6).strip(), #Material Description
            'hsn_code': item_match.group(7), #HSN Code
            'uom': item_match.group(8), #UOM
            'stc_charges': item_match.group(9), #STC Charges
            'basic_rate': item_match.group(10), #Basic Rate
            'billed_quantity': item_match.group(11), #Billed Quantity
        }
    return None

def extract_table_data_wcl_rail(text):
    # Split the text into lines and remove empty lines
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    # Initialize containers
    table = []
    totals = None
    
    # Process each line
    for line in lines[1:]:  # Skip header
        parts = line.split()
        if parts[0].strip().upper() == 'TOTAL':
            # Store totals
            totals = {
                'TARE': float(parts[1]),
                'GROSS': float(parts[2]),
                'NET_WT': float(parts[3]),
                'UNDERLOAD': float(parts[4]),
                'OVERLOAD': float(parts[5])
            }
            continue
            
        if len(parts) >= 10:
            try:
                # Convert numeric values to float where appropriate
                row = {
                    'SL_NO': int(parts[0].strip()),
                    'WAGON_NO': parts[1],
                    'TYPE_OF_BOX': parts[2],
                    'CARRY_CAP': float(parts[3]),
                    'PER_CC': float(parts[4]),
                    'TARE': float(parts[5]),
                    'GROSS': float(parts[6]),
                    'NET_WT': float(parts[7]),
                    'UNDERLOAD': float(parts[8]),
                    'OVERLOAD': float(parts[9])
                }
                table.append(row)
            except ValueError as e:
                continue
        
    return {
        'table': table,
        'total': totals
    }




def extract_fields_wcl_rail(pdf_path):
    reader = PdfReader(pdf_path)
    
    text = ""
    for page in reader.pages:
        text += page.extract_text()
    fields = {} 
    # print(text)

    with open(pdf_path, 'rb') as pdfFileObj:
        pdfReader = PyPDF3.PdfFileReader(pdfFileObj,strict=False)
        pageObj = pdfReader.pages[0]
        mytext = pageObj.extractText()  
    # print(mytext)

    #secl insert

    # Adjusted regex patterns for more accurate extraction

    fields["area_code"] = extract_with_regex_wcl_rail(r':?\s*([A-Z0-9]+)\s*Area Code\s*', text) # Area Code
    fields["area_description"] = extract_with_regex_wcl_rail(r':?\s*([A-Za-z]+)\s*Area Description\s*', text) #Area Description
    fields["invoice_no"] = extract_with_regex_wcl_rail(r'Supplier Phone\s*:?\s*([0-9]+)', text) #Invoice No
    fields["invoice_date"] = extract_with_regex_wcl_rail(r'([A-Za-z]{3} \d{1,2}, \d{4})\s+Invoice Date\s*:', text) #Invoice Date
    fields["contract_reference"] = extract_with_regex_wcl_rail(r'([0-9]+)\s*Contract Reference:?', text) #Contract Reference
    fields["contract_type"] = extract_with_regex_wcl_rail(r'(.*?)\s+Contract type\s*:', text) #Contract type
    fields["sales_order"] = extract_with_regex_wcl_rail(r'([0-9]+)\s*Sales Order:?', text) #Sales Order
    fields["sale_order_date"] = extract_with_regex_wcl_rail(r'\s*(\w+\s+\d{1,2},\s+\d{4})\s*Sale Order Date', text) #Sale Order Date
    fields["delivery_number"] = extract_with_regex_wcl_rail(r'([0-9]+)\s*Delivery Number\s*:?', text) #Delivery Number
    fields["mode_of_dispatch"] = extract_with_regex_wcl_rail(r'(\w+)\s+Mode Of Dispatch\s*:', text) #Mode Of Dispatch
    fields["rr_no"] = extract_with_regex_wcl_rail(r'\s*:\s*(\d+)\s*RR_NO\s*:', text) #RR_NO
    fields["rr_date"] = extract_with_regex_wcl_rail(r'\s*(\d{4}-\d{1,2}-\d{1,2})\s*RR_DATE\s*:?', text) #RR_Date
    fields["net_weight"] = extract_with_regex_wcl_rail(r'([,0-9.]+)\s*Net Weight\s*:?', text) #Net Weight
    fields["dnote_no"] = extract_with_regex_wcl_rail(r'([0-9]+)\s*Dnote No\s*', text) #Dnote No
    fields["dnote_date"] = extract_with_regex_wcl_rail(r'\s*:?(\d{4}\d{1,2}\d{1,2})\s*Dnote_DATE\s*:?', text) #Dnote_DATE
    fields["siding"] = extract_with_regex_wcl_rail(r'Date\s*:(.*?)Siding', text) #Siding
    fields["loading_date"] = extract_with_regex_wcl_rail(r':?([A-Za-z]{3} \d{1,2}, \d{4})\s+Loading Date\s*', text) #Loading Date
    fields["rake_sq_no"] = extract_with_regex_wcl_rail(r'\s*(\d+)\s*Rake sq. no.\s*', text) #Rake sq no
    fields["sanction_no"] = extract_with_regex_wcl_rail(r'([A-Za-z0-9/]+)\s+Sanction No.\s*', text) #Sanction No

    fields['bill'] = extract_values_wcl_rail(mytext)

    fields['particulars'] = extract_invoice_data_wcl_rail(text)

    fields['table_extract'] = extract_table_data_wcl_rail(text)
    return fields


# if __name__=="__main__":
#     import os
#     folder_path=os.path.join(os.getcwd(),"pdf")

#     files=os.listdir(folder_path)
#     for file in files:
#         print(file,"********************************************")
#         extracted_fields = extract_fields_wcl_rail(os.path.join(folder_path,file))
#         print(extracted_fields)

   