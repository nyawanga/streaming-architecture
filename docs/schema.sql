/* Schemas DDL for franco */
/* PostgreSQL Database (Source) */
CREATE DATABASE franco;
/* Customer table */
CREATE TABLE franco.Customer(
Customer_Id SERIAL NOT NULL,
Last_Name varchar(50) NULL,
Address_Line1 varchar(50) NULL,
Address_Line2 varchar(50) NULL,
Birth_Date varchar(50) NULL,
Age varchar(50) NULL,
Commute_Distance varchar(50) NULL,
Customer_Alternate_Key varchar(50) NULL,
Customer_Key varchar(50) NULL,
Date_First_Purchase varchar(50) NULL,
Email_Address varchar(50) NULL,
English_Education varchar(50) NULL,
English_Occupation varchar(50) NULL,
French_Education varchar(50) NULL,
First_Name varchar(50) NULL,
French_Occupation varchar(50) NULL,
Gender varchar(50) NULL,
House_Owner_Flag varchar(50) NULL,
Marital_Status varchar(50) NULL,
Middle_Name varchar(50) NULL,
Name_Style varchar(50) NULL,
Number_Cars_Owned varchar(50) NULL,
Number_Children_At_Home varchar(50) NULL,
Phone varchar(50) NULL,
Spanish_Education varchar(50) NULL,
Spanish_Occupation varchar(50) NULL,
Suffix varchar(50) NULL,
Title varchar(50) NULL,
Total_Children varchar(50) NULL,
Yearly_Income varchar(50) NULL,
PRIMARY KEY (Customer_Id)
);
/* Sales_Territory table */
CREATE TABLE franco.Sales_Territory(
Sales_Territory_Id SERIAL NOT NULL,
Sales_Territory_Country varchar(50) NULL,
Sales_Territory_Region varchar(50) NULL,
Sales_Territory_City varchar(50) NULL,
PRIMARY KEY (Sales_Territory_Id)
);
/* Employee table */
CREATE TABLE franco.Employee(
Employee_Id SERIAL NOT NULL,
Employee_Name varchar(50) NULL,
Employee_Territory_Region varchar(50) NULL,
PRIMARY KEY (Employee_Id)
);
/* Sales table */
CREATE TABLE franco.Sales(
Sales_Id SERIAL NOT NULL,
CurrencyKey varchar(50) NULL,
CustomerKey varchar(50) NULL,
Discount_Amount varchar(50) NULL,
DueDate varchar(50) NULL,
DueDateKey varchar(50) NULL,
Extended_Amount varchar(50) NULL,
Freight varchar(50) NULL,
Order_Date varchar(50) NULL,
Order_Quantity varchar(50) NULL,
Product_Standard_Cost varchar(50) NULL,
Revision_Number varchar(50) NULL,
Sales_Amount varchar(50) NULL,
Sales_Order_Line_Number varchar(50) NULL,
Sales_Order_Number varchar(50) NULL,
SalesTerritoryKey varchar(50) NULL,
ShipDate varchar(50) NULL,
Tax_Amt varchar(50) NULL,
Total_Product_Cost varchar(50) NULL,
Unit_Price varchar(50) NULL,
Unit_Price_Discount_Pct varchar(50) NULL,
Employee_Id INT NOT NULL,
PRIMARY KEY (Sales_Id),
FOREIGN KEY (Employee_Id) REFERENCES Employee (Employee_Id)
);
/* END */
