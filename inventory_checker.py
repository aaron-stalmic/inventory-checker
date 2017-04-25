import pyodbc
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from stalmic_settings import *


connect_string = 'DRIVER={SQL Server}; SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password
stalmic = pyodbc.connect(connect_string)
cursor = stalmic.cursor()
stalmic.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
stalmic.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
stalmic.setencoding('utf-8')
scope = ['https://spreadsheets.google.com/feeds']
credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
gc = gspread.authorize(credentials)


def getTotalQty(item, warehouse):
    totalqtycommand = """
    SELECT dbo.Inventory.Description, WarehouseNum, QtyOnHand
    FROM dbo.InventoryWarehouse
    INNER JOIN dbo.Inventory
    ON dbo.Inventory.InventoryID = dbo.InventoryWarehouse.InventoryID
    INNER JOIN dbo.Warehouse
    ON dbo.Warehouse.WarehouseID = dbo.InventoryWarehouse.WarehouseID
    WHERE dbo.Inventory.Description = ? AND WarehouseNum = ?"""
    cursor.execute(totalqtycommand, (item, warehouse))
    if totalqty is not None:
        totalqty = float(cursor.fetchone()[2])
    else:
        totalqty = 0
    return totalqty


def getTruckQty(item, truck):
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
    WHERE dbo.Inventory.Description = ? AND dbo.Truck.TruckNum = ?"""
    cursor.execute(truckqtycommand, (item, truck))
    truckqty = cursor.fetchone()
    if truckqty is not None:
        truckqty = float(truckqty[2])
    else:
        truckqty = 0
    return truckqty


def getOnHandQty(item, warehouse, trucklist):
    totalqty = getTotalQty(item, warehouse)
    truckqtylist = [getTruckQty(item, truck) for truck in trucklist]
    return totalqty - sum(truckqtylist)
