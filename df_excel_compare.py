import pandas as pd
import xlsxwriter
import string
import io

from datetime import datetime
from copy import deepcopy

class compare():
    
    def __init__(self, df_dict:dict, copy = True, sheet_name = None):
        
        assert isinstance(df_dict, dict), 'Please input a dictionary of dataframes'
        assert len(df_dict) == 2, 'Too many inputs. Please only input two dataframes'
        for x in df_dict.values():
            assert isinstance(x, pd.DataFrame), f'Please input {x} as pd.DataFrame'
        
        self.sheet_name = f"{' - '.join(df_dict)}_comparison" if not sheet_name else str(sheet_name)
        self.df_dict = df_dict
        self.config = {x:{} for x in self.df_dict}
        self.error_split_string = ' <-{} | {}-> '.format(*self.df_dict)
        self.columns = list(set(list(self.df_dict.values())[0].columns) & set(list(self.df_dict.values())[1].columns))
    
    def __add__(self, other):
        
        assert isinstance(other, type(self)), 'Both should be compare class objects'
        
        _temp = type(self)
    
    def _process_dfs(self, join_on):
        
        for x, df in self.df_dict.items():
            
            for col, func in self.config[x].items():
                
                self.df_dict[x][col] = self.df_dict[x][col].apply(func)
                
            self.df_dict[x] = self.df_dict[x].astype(str).sort_values(by = join_on)


    def _get_excel_range(self, length: int, width: int, columns: bool = True) -> str:
        
        letters = list(string.ascii_uppercase) + [f"{x}{y}" 
                                                  for x in string.ascii_uppercase
                                                  for y in string.ascii_uppercase]
        
        return f"A{int(columns)+1}:{letters[width]}{length+1}"
    
    def _excel_column_width(self, df: pd.DataFrame, with_col: bool = True, default = 8.43) -> dict:
        
        return {i:round(min(1.0528*max([len(str(x)) for x in df[i]] + [len(i) if with_col else default])+2.2974, 80), 2) for i in df.columns}

    def _fill_func(self, x, y, col):
        
        if x == y:
            return x
        else:
            self.errors[col] += 1
            return f"{x}{self.error_split_string}{y}"
    
    def _comp(self, df1 : pd.DataFrame, df2 : pd.DataFrame, join_on : list):
        
        inner_cols = list(filter(lambda x: x not in join_on, self.columns))
        df1 = df1.rename(columns = {x:f'{x}_df1' for x in inner_cols})
        df2 = df2.rename(columns = {x:f'{x}_df2' for x in inner_cols})
        
        if join_on:
            df_new = df1.merge(df2, on = join_on, how = 'outer', indicator = True)
        else:
            df_new = df1.merge(df2, left_index = True, right_index = True, how = 'outer', indicator = True)
            
        for x in inner_cols:
            
            df_new[x] = df_new.apply(lambda i: self._fill_func(i[f'{x}_df1'], i[f'{x}_df2'], x), axis = 1)
            df_new.drop([f'{x}_df1', f'{x}_df2'], axis = 1, inplace = True)
            
        return df_new
    
    def set_config(self, config : dict):
        
        assert isinstance(config, dict), 'Please input config as a dictionary'
        assert set(config).issubset(set(self.df_dict)), "The following keys are not recognised: {', '.join(set(config) - set(df_dict))}"      
        
        self.config.update(config)
    
    
    def run(self, join_on : list):
        
        self.errors = {x:0 for x in filter(lambda x: x not in join_on, self.columns)}
        
        self._process_dfs(join_on = join_on)
        self._comp_results = self._comp(*self.df_dict.values(), join_on=join_on)
        
        self.status = 'OK' if set(list(map(lambda x: len(x),self.df_dict.values())) +
                                  [len(self._comp_results)]) == {1} else 'NOT OK'
    
    def save_as_excel(self, file):
        
        assert '_comp_results' in dir(self), 'Please run the check with .run() first'
        assert file.endswith('.xlsx'), 'Please make sure that you include .xlsx at the end of the file name'
        
        with io.BytesIO() as out:
            
            with pd.ExcelWriter(out, engine = 'xlsxwriter') as writer:
                
                workbook = writer.book
                
                #Light red fill with dark red text
                format_red = workbook.add_format({'bg_color' : '#FFC7CE',
                                               'font_color' : '#9C0006'})
                    
                self._comp_results.to_excel(writer, self.sheet_name, index = False)
                worksheet = writer.sheets[self.sheet_name]
                
                if any((self.errors, self.status == 'NOT OK')):
                    
                    worksheet.conditional_format(self._get_excel_range(*self._comp_results.shape),
                                                 {'type'     : 'text',
                                                  'criteria' : 'containing',
                                                  'value'    : self.error_split_string,
                                                  'format'   : format_red})
                    
                    worksheet.set_tab_color('#FFC7CE') #set tab color to red if any errors detected
                
                else:
                    
                    worksheet.set_tab_color('#C6EFCE')
                
                for x,y in enumerate(self._excel_column_width(self._comp_results).values()):
                    worksheet.set_column(x, x, y)
                    
                    
            self.out_data = out.getvalue()
    
        with open(file, 'wb') as t:
            t.write(self.out_data)
