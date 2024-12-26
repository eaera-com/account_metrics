import csv

header_mapping = {
    "deal_id": "Deal",
    "external_id": "ExternalID",
    "login": "Login",
    "dealer": "Dealer",
    "order_id": "Order",
    "action": "Action",
    "entry": "Entry",
    "digits": "Digits",
    "digits_currency": "DigitsCurrency",
    "contract_size": "ContractSize",
    "symbol": "Symbol",
    "volume": "Volume",
    "profit": "Profit",
    "storage": "Storage",
    "commission": "Commission",
    "rate_profit": "RateProfit",
    "rate_margin": "RateMargin",
    "expert_id": "ExpertID",
    "position_id": "PositionID",
    "comment": "Comment",
    "profit_raw": "ProfitRaw",
    "reason": "Reason",
    "gateway": "Gateway",
    "price_gateway": "PriceGateway",
    "fee": "Fee",
    "value": "Value",
    "server": "server"
}


def read_and_convert_csv(input_file_path, output_file_path, header_mapping):
    with open(input_file_path, mode='r') as infile, open(output_file_path, mode='w', newline='') as outfile:
        csv_reader = csv.DictReader(infile, delimiter=',')
        fieldnames = [header_mapping.get(field, field)
                      for field in csv_reader.fieldnames]
        csv_writer = csv.DictWriter(outfile, fieldnames=fieldnames)

        csv_writer.writeheader()
        cnt = 0
        for row in csv_reader:
            cnt += 1
            if cnt > 30:
                break
            modified_row = {header_mapping.get(
                key, key): value for key, value in row.items()}
            csv_writer.writerow(modified_row)


if __name__ == "__main__":
    input_file_path = 'auda_deals_100000.csv'
    output_file_path = 'modified_auda_deals_100000.csv'
    read_and_convert_csv(input_file_path, output_file_path, header_mapping)
