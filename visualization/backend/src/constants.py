class ConstantProvider:
    @staticmethod
    def pie_chart_name():
        return "pie"

    @staticmethod
    def line_chart_name():
        return "line"

    @staticmethod
    def bar_chart_name():
        return "bar"

    @staticmethod
    def map_chart_name():
        return "map"

    @staticmethod
    def map_region_chart_name():
        return "map_region"
    
    @staticmethod
    def fact_kpi_sale_amount():
        return "Sales Amount"

    @staticmethod
    def fact_kpi_quantity():
        return "Quantity"

    @staticmethod
    def dim_address_name():
        return "dimaddress"

    @staticmethod
    def dim_date_name():
        return "dimdate"

    @staticmethod
    def fact_name():
        return "fctsales"

    @staticmethod
    def dim_customer_name():
        return "dimcustomer"

    @staticmethod
    def dim_product_name():
        return "dimproduct"

    @staticmethod
    def dim_date_quarter_column():
        return "quarter_of_year"

    @staticmethod
    def dim_date_year_column():
        return "year_number"

    @staticmethod
    def fact_name():
        return "fctsales"

    @staticmethod
    def SCD_dims_list():
        return ["dimcustomer", "dimaddress", "dimpromotion"]

    @staticmethod
    def valid_dim_condition(dim_table: str):
        return f"{dim_table}.is_valid = 1"

    @staticmethod
    def chunk_size():
        return 10**6

    @staticmethod
    def fact_col_alias():
        return "fact_col"

    @staticmethod
    def first_dim_col_alias():
        return "dim_1_col"

    @staticmethod
    def sec_dim_col_alias():
        return "dim_2_col"

    @staticmethod
    def total_sales_amount_query():
        return f"""
            SELECT SUM(sales_amount) AS totalSalesAmount
            FROM {ConstantProvider.fact_name()}
        """

    @staticmethod
    def total_orders_query():
        return f"""
            SELECT SUM(order_qty) AS totalOrders
            FROM {ConstantProvider.fact_name()}
        """

    @staticmethod
    def total_customers_query():
        return f"""
            SELECT COUNT(*) AS totalCustomers 
            FROM {ConstantProvider.dim_customer_name()}
            WHERE is_valid = 1
        """

    @staticmethod
    def total_products_query():
        return f"""
            SELECT COUNT(*) AS totalProducts
            FROM {ConstantProvider.dim_product_name()}
            WHERE is_valid =  1
        """
