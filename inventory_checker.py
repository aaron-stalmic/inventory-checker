import pyodbc
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from stalmic_settings import *


class InvItem:
    def __init__(self, item):
        if item[-3:] == '...':
            item = item[:-3]
        self.item = item
        connect_string = 'DRIVER={SQL Server}; SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password
        self.stalmic = pyodbc.connect(connect_string)
        self.cursor = self.stalmic.cursor()
        self.stalmic.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        self.stalmic.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        self.stalmic.setencoding('utf-8')

    def getTotalQty(self, warehouse):
        totalqtycommand = """
        SELECT dbo.Inventory.Description, WarehouseNum, QtyOnHand
        FROM dbo.InventoryWarehouse
        INNER JOIN dbo.Inventory
        ON dbo.Inventory.InventoryID = dbo.InventoryWarehouse.InventoryID
        INNER JOIN dbo.Warehouse
        ON dbo.Warehouse.WarehouseID = dbo.InventoryWarehouse.WarehouseID
        WHERE dbo.Inventory.Description LIKE ? AND WarehouseNum = ?"""
        self.cursor.execute(totalqtycommand, (self.item+'%', warehouse))
        totalqty = self.cursor.fetchone()
        if totalqty is not None:
            totalqty = float(totalqty[2])
        else:
            totalqty = 0
        return totalqty

    def getTruckQty(self, truck):
        truckqtycommand = """
        SELECT dbo.Inventory.Description, dbo.Truck.TruckNum, it.EndTruckQty
        FROM dbo.Truck
        INNER JOIN
        (
            SELECT a.TruckID, dbo.RouteLoad.InventoryID, dbo.RouteLoad.EndTruckQty, dbo.RouteLoad.EndUnits, a.RouteID, a.RouteDate, a.RouteDayID
            FROM dbo.RouteLoad
            INNER JOIN
            (
                SELECT Row_Number() OVER(PARTITION BY dbo.RouteDay.TruckID ORDER BY dbo.RouteDay.RouteDate DESC, dbo.Shift.ShiftNum DESC) rownum, RouteDayID, TruckID, RouteDate, RouteID
                FROM dbo.RouteDay
                LEFT JOIN dbo.Shift ON dbo.Shift.ShiftID = dbo.RouteDay.ShiftID
                WHERE dbo.RouteDay.RouteDate <DATEADD(DAY, 1, SYSDATETIME())
            ) a ON a.RouteDayID = dbo.RouteLoad.RouteDayID AND a.rownum = 1
            WHERE dbo.RouteLoad.EndTruckQty <> 0
        ) it ON it.TruckID = dbo.Truck.TruckID
        INNER JOIN dbo.Inventory ON dbo.Inventory.InventoryID = it.InventoryID
        WHERE dbo.Inventory.Description LIKE ? AND dbo.Truck.TruckNum = ?"""
        self.cursor.execute(truckqtycommand, (self.item+'%', truck))
        truckqty = self.cursor.fetchone()
        if truckqty is not None:
            truckqty = float(truckqty[2])
        else:
            truckqty = 0
        return truckqty

    def getOnHandQty(self, warehouse, trucklist):
        totalqty = self.getTotalQty(warehouse)
        truckqtylist = [self.getTruckQty(truck) for truck in trucklist]
        return totalqty - sum(truckqtylist)

    def closeConn(self):
        self.stalmic.close()


class WHSheet:
    def __init__(self, sheet, worksheet):
        scope = ['https://spreadsheets.google.com/feeds']
        credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        gc = gspread.authorize(credentials)
        sh = gc.open(sheet)
        self.ws = sh.worksheet(worksheet)

    def getCol(self, col):
        collist = self.ws.col_values(col)
        length = len(collist) - 1
        while collist[length] == '':
            length -= 1
        return collist[0:length+1]

    def setValue(self, r, c, value):
        self.ws.update_cell(r, c, value)



# lakeland = WHSheet('Lakeland Warehouse Inventory Sheet', 'Lakeland Count Sheet')

if __name__ == '__main__':
    columns = [1, 5, 10]

    townsend = WHSheet('Townsend Warehouse Inventory Sheet', 'Townsend Count Sheet')
    townsend_trucklist = ['TRLR-Townsend', 'SAPELO', 'SAW DOC', 'SAW MAN', 'STEVE', 'BIRO SAW', 'GARY', 'SAWMAN2', 'ETYC38']
    lakeland = WHSheet('Lakeland Warehouse Inventory Sheet', 'Lakeland Count Sheet')
    lakeland_trucklist = ['TRLR-Lakeland', 'DIGI MON', 'SAWMAN3', 'STALMIC']
    wh_list = [[townsend, townsend_trucklist, 'WH#1 Townsend'],
               [lakeland, lakeland_trucklist, 'WH#2 Lakeland']]

    for wh in wh_list:
        for c in columns:
            itemlist = wh[0].getCol(c)[1:]
            for r in range(len(itemlist)):
                if itemlist[r] != '':
                    totalqty = InvItem(itemlist[r]).getTotalQty(wh[2])
                    qty = InvItem(itemlist[r]).getOnHandQty(wh[2], wh[1])
                    wh[0].setValue(r+2, c+2, qty)
                    print("Updating {}: {}".format(wh[2], itemlist[r]))
                    print("{} - {} = {}".format(int(totalqty), int(totalqty - qty), int(qty)))
