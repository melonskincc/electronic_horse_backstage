import xlwt
class GenExcel():
    '''生成excel类'''
    def __init__(self,sheet:str):
        self.sheetName=sheet
        self.f=xlwt.Workbook(encoding="utf-8")  # 创建工作簿
        self.sheet1=self.f.add_sheet(sheet, cell_overwrite_ok=True) # 创建sheet
        self.head_style=self.__set_style('Times New Roman', 220, True)
        self.row_style=self.__set_style('Times New Roman', 220, False)
        self.starNum=1
        self.endNum=0

    def __set_style(self,name, height, bold=False):
        """设置样式"""
        style = xlwt.XFStyle()  # 初始化样式
        font = xlwt.Font()  # 为样式创建字体
        font.name = name  # 'Times New Roman'
        font.bold = bold
        font.color_index = 4
        font.height = height

        # borders= xlwt.Borders()
        # borders.left= 6
        # borders.right= 6
        # borders.top= 6
        # borders.bottom= 6

        style.font = font
        # style.borders = borders

        return style

    def write_excel_row0(self,row0:list):
        """写第一行数据"""
        for i in range(0,len(row0)):
            self.sheet1.write(0,i,row0[i],self.head_style)

    def write_excel_data(self,rowsData:list):
        """写每一行数据"""
        self.endNum+=len(rowsData)
        for i,rowNum in enumerate(range(self.starNum,self.endNum)):
            for colNum,data in enumerate(rowsData[i].values()):
                self.sheet1.write(rowNum, colNum, data, self.row_style)
        self.starNum=self.endNum

    def save_file(self):
        self.f.save('../excel/{}.xls'.format(self.sheetName))  # 保存文件

    def return_bytes(self):
        return self.f.get_biff_data()
