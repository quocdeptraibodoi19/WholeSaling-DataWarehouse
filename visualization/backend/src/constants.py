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
    def dim_date_quarter_column():
        return "quarter_of_year"
    
    @staticmethod
    def dim_date_year_column():
        return "year_number"
    
    @staticmethod
    def fact_name():
        return "fctsales"
    
    @staticmethod
    def chunk_size():
        return 10**6
    
    @staticmethod
    def fact_col_alias():
        return 'fact_col'

    @staticmethod
    def first_dim_col_alias():
        return "dim_1_col"
    
    @staticmethod
    def sec_dim_col_alias():
        return "dim_2_col"