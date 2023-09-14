create or replace package pkg_planning_tool_pan_india as

  -- Author  : POOJA SHREE JAIN
  -- Created : 07/NOV/2022 11:33:14 AM
  -- Purpose : NEW PLANNING TOOL
  
   PROCEDURE SP_TRAN_DEMAND_CO(P_FROM_DATE     IN DATE,
                              P_TO_DATE       IN DATE);

  PROCEDURE SP_TRAN_DEMAND_WORKSHOP(P_FROM_DATE     IN DATE,
                                    P_TO_DATE       IN DATE);

  PROCEDURE SP_TRAN_DEMAND_INDENT(P_FROM_DATE     IN DATE,
                                  P_TO_DATE       IN DATE);

  PROCEDURE SP_TRAN_DEMAND_SUMMARY;
   PROCEDURE SP_CHD_PO_3YRS_NOT_INDT(P_FROM_DATE IN DATE,
                                    P_TO_DATE   IN DATE,
                                    P_ERR_CD    OUT NUMBER,
                                    P_ERR_MSG   OUT VARCHAR2);
  PROCEDURE SP_CHD_PO_3YRS_INDT(P_FROM_DATE IN DATE,
                                P_TO_DATE   IN DATE,
                                P_ERR_CD    OUT NUMBER,
                                P_ERR_MSG   OUT VARCHAR2);
  PROCEDURE SP_CHD_PO_3YRS_CO(P_FROM_DATE IN DATE,
                              P_TO_DATE   IN DATE,
                              P_ERR_CD    OUT NUMBER,
                              P_ERR_MSG   OUT VARCHAR2);

  PROCEDURE SP_MOTHER_CL03_DATA_3YRS(P_FROM_DATE IN DATE,
                                     P_TO_DATE   IN DATE,
                                     P_ERR_CD    OUT NUMBER,
                                     P_ERR_MSG   OUT VARCHAR2);

  PROCEDURE SP_MOTHER_DMS_DATA_3YRS(P_FROM_DATE IN DATE,
                                    P_TO_DATE   IN DATE,
                                    P_ERR_CD    OUT NUMBER,
                                    P_ERR_MSG   OUT VARCHAR2);
                                   
   PROCEDURE SP_PART_SELECTION;

  PROCEDURE SP_ITEM_DATA(P_ERR_CD OUT NUMBER, P_ERR_MSG OUT VARCHAR2);

  PROCEDURE SP_STOCK_DATA(P_ERR_CD OUT NUMBER, P_ERR_MSG OUT VARCHAR2);

  PROCEDURE SP_SPR_CBO_NEW(P_ERR_CD OUT NUMBER, P_ERR_MSG OUT VARCHAR2);

  PROCEDURE SP_REPLACEMENT_DATA(
                                P_ERR_CD    OUT NUMBER,
                                P_ERR_MSG   OUT VARCHAR2);

  PROCEDURE SP_SUPPLIER_DATA(
                             P_ERR_CD           OUT NUMBER,
                             P_ERR_MSG          OUT VARCHAR2);

 

  
end pkg_planning_tool_pan_india;
/
create or replace package body pkg_planning_tool_pan_india as

  PROCEDURE SP_TRAN_DEMAND_CO(P_FROM_DATE IN DATE, P_TO_DATE IN DATE) IS
  
  BEGIN
  
    BEGIN
      Insert into PH_TRANS_DEMAND_CO_POC
        (Raw_Transaction_Type,
         Item_Code,
         Prefix,
         Warehouse_code,
         Warehouse_Grp_Cd,
         Sales_Order_Num,
         Sales_Ord_Line_Num,
         Extraction_date,
         Ordered_quantity,
         Supplied_quantity,
         Demand_Date,
         Customer_Code,
         Use_For_Forcasting,
         Use_For_Service_Level,
         Use_For_Classification,
         Stock_demand_date,
         Sales_Type,
         Job_Card_Number,
         Job_Card_Status,
         Party_Type,
         Service_Type,
         Use_For_Sales_Value_KPI,
         Demand_Stream,
         Direct_Demand,
         Order_Date,
         Created_Date)
        Select Raw_Transaction_Type,
               Item_Code,
               '' Prefix,
               Warehouse_code,
               Warehouse_Grp_Cd,
               Sales_Order_Num,
               Sales_Ord_Line_Num,
               Extraction_date,
               sum(Ordered_Qty - return_qty - Cancel_Qty) Ordered_quantity,
               sum(Sale_Qty - return_qty) Supplied_quantity,
               Demand_Date,
               Customer_Code,
               Use_For_Forcasting,
               Use_For_Service_Level,
               Use_For_Classification,
               Stock_demand_date,
               Sales_Type,
               Job_Card_Number,
               Max(Job_Card_Status),
               Party_Type,
               Service_Type,
               Use_For_Sales_Value_KPI,
               Demand_Stream,
               Direct_Demand,
               TO_DATE(Order_Date, 'dd-mm-yyyy'), 
               SYSDATE Created_date
          from (SELECT /*+ Index(PD PK_PD_SO,AM XPKAM_DEALER_LOC)+*/
                 'M' Raw_Transaction_Type,
                 PD.PART_NUM Item_Code,
                 '' Prefix,
                 AM.PARENT_GROUP || '-' || AM.Dealer_Map_Cd || '-' ||
                 AM.LOC_CD Warehouse_code,
                 AM.Region_cd || '_' || AM.PARENT_GROUP Warehouse_Grp_Cd,
                 PH.SO_NUM Sales_Order_Num,
                 AM.PARENT_GROUP || AM.Dealer_Map_Cd || AM.LOC_CD ||
                 PH.SO_NUM || PD.PART_NUM || to_char(PH.So_Date, 'YYMMDD') || 'R' Sales_Ord_Line_Num,
                 to_char(sysdate, 'YYYYMMDD') || 'T' ||
                 to_char(sysdate, 'HH24MI') Extraction_date,
                 nvl(PD.SO_QTY, 0) Ordered_Qty,
                 0 Sale_Qty,
                 0 Cancel_Qty,
                 0 Return_Qty,
                 to_char(PH.So_Date, 'YYYYMMDD') Demand_Date,
                 '' Customer_Code,
                 'N' Use_For_Forcasting,
                 case
                   when Ph.Party_Type in ('D', 'DI') then
                    'N'
                   else
                    ''
                 end as Use_For_Service_Level,
                 'N' Use_For_Classification,
                 '' Stock_demand_date,
                 'Counter Sale' Sales_Type,
                 '' Job_Card_Number,
                 '' Job_Card_Status,
                 PH.Party_Type Party_Type,
                 '' Service_Type,
                 'N' Use_For_Sales_Value_KPI,
                 '' Demand_Stream,
                 '' Direct_Demand,
                 '' Order_date
                  FROM PH_SO             PH,
                       PD_SO             PD,
                       AM_DEALER_LOC     AM,
                       am_company_master ma
                 WHERE PH.DEALER_MAP_CD = PD.DEALER_MAP_CD
                   AND PH.LOC_CD = PD.LOC_CD
                   AND PH.PARENT_GROUP = PD.PARENT_GROUP
                   AND PH.SO_NUM = PD.SO_NUM
                   AND PH.COMP_FA = PD.COMP_FA
                   AND PD.SRL_NUM > 0
                   AND PH.DEALER_MAP_CD = AM.DEALER_MAP_CD
                   AND PH.LOC_CD = AM.LOC_CD
                   AND PH.PARENT_GROUP = AM.PARENT_GROUP
                   AND PH.Comp_Fa = MA.Comp_Code
                   and TRUNC(PH.So_Date) >= P_FROM_DATE
                   and TRUNC(PH.So_Date) < P_TO_DATE
                   and ma.parent_group = AM.parent_group
                   and ma.dealer_map_cd = AM.dealer_map_cd
                   and AM.principal_map_cd = 1
                UNION ALL
                SELECT /*+Index( ph IDX_PH_ISSUE_DATE,AM XPKAM_DEALER_LOC)+*/
                 'M' Raw_Transaction_Type,
                 PDO.Part_Num Item_Code,
                 '' Prefix,
                 AM.PARENT_GROUP || '-' || AM.Dealer_Map_Cd || '-' ||
                 AM.LOC_CD Warehouse_code,
                 AM.Region_cd || '_' || AM.PARENT_GROUP Warehouse_Grp_Cd,
                 PSO.SO_NUM Sales_Order_Num,
                 AM.PARENT_GROUP || AM.Dealer_Map_Cd || AM.LOC_CD ||
                 PSO.SO_NUM || PDO.PART_NUM ||
                 to_char(PSO.So_Date, 'YYMMDD') || 'R' Sales_Ord_Line_Num,
                 to_char(sysdate, 'YYYYMMDD') || 'T' ||
                 to_char(sysdate, 'HH24MI') Extraction_date,
                 0 Ordered_qty,
                 nvl(PD.Bill_Qty, 0) Sale_Qty,
                 0 Cancel_Qty,
                 nvl(pd.ret_qty, 0) Return_qty,
                 to_char(pso.so_date, 'YYYYMMDD') Demand_Date,                 
                 '' Customer_Code,
                 'N' Use_For_Forcasting,
                 case
                   when Ph.Party_Type in ('D', 'DI') then
                    'N'
                   else
                    ''
                 end as Use_For_Service_Level,
                 'N' Use_For_Classification,
                 '' Stock_demand_date,
                 'Counter Sale' Sales_Type,
                 '' Job_Card_Number,
                 to_char(PH.doc_date, 'YYYYMMDD') Job_Card_Status,
                 PH.Party_Type Party_Type,
                 '' Service_Type,
                 'N' Use_For_Sales_Value_KPI,
                 '' Demand_Stream,
                 '' Direct_Demand,
                 '' Order_date
                  from PH_ISSUE          ph,
                       PD_ISSUE          pd,
                       am_dealer_loc     AM,
                       pm_part           pm,
                       pm_part           pm1,
                       am_company_master ma,
                       PH_SO             PSO,
                       PD_SO             PDO               
                 where Ph.DOC_TYPE IN ('RSI', 'CSI')
                   AND ph.parent_group = AM.parent_group
                   and ph.dealer_map_cd = AM.dealer_map_cd
                   and ph.loc_cd = AM.loc_cd
                   and ph.comp_fa = ma.comp_code
                   and (ph.doc_date) >= P_FROM_DATE
                   and (ph.doc_date) < P_TO_DATE
                   and TRUNC(PSO.So_Date) >= P_FROM_DATE
                   and TRUNC(PSO.So_Date) < P_TO_DATE
                   and pd.parent_group = ph.parent_group
                   and pd.dealer_map_cd = ph.dealer_map_cd
                   and pd.loc_cd = ph.loc_cd
                   and pd.comp_fa = ph.comp_fa
                   and pd.doc_type = ph.doc_type
                   and pd.doc_num = ph.doc_num
                   and pd.srl_num >= 0
                   AND PM.PART_NUM = pd.PART_NUM
                   AND PM.DEALER_MAP_CD = 1
                   and ma.parent_group = AM.parent_group
                   and ma.dealer_map_cd = AM.dealer_map_cd
                   and AM.principal_map_cd = 1
                   AND PSO.DEALER_MAP_CD = PDO.DEALER_MAP_CD
                   AND PSO.LOC_CD = PDO.LOC_CD
                   AND PSO.PARENT_GROUP = PDO.PARENT_GROUP
                   AND PSO.SO_NUM = PDO.SO_NUM
                   AND PSO.COMP_FA = PDO.COMP_FA
                   and pdo.part_num = pd.part_num
                   AND PDO.SRL_NUM > 0
                   and pso.parent_group = ph.parent_group
                   and pso.dealer_map_cd = ph.dealer_map_cd
                   and pso.loc_cd = ph.loc_cd
                   and pso.comp_fa = ph.comp_fa
                   and pso.so_num = pd.ref_doc_num
                   AND PM.PART_NUM = pd.PART_NUM
                   AND PM.DEALER_MAP_CD = 1
                   AND PM1.PART_NUM = pdo.PART_NUM
                   AND PM1.DEALER_MAP_CD = 1
                
                UNION ALL
                SELECT /*+ Index(PD PK_PD_SO,AM XPKAM_DEALER_LOC)+*/
                 'M' Raw_Transaction_Type,
                 PD.PART_NUM Item_Code,
                 '' Prefix,
                 AM.PARENT_GROUP || '-' || AM.DEALER_MAP_CD || '-' ||
                 AM.LOC_CD Warehouse_code,
                 AM.Region_cd || '_' || AM.PARENT_GROUP Warehouse_Grp_Cd,
                 PH.SO_NUM Sales_Order_Num,
                 AM.PARENT_GROUP || AM.Dealer_Map_Cd || AM.LOC_CD ||
                 PH.SO_NUM || PD.PART_NUM || to_char(PH.So_Date, 'YYMMDD') || 'R' Sales_Ord_Line_Num,
                 to_char(sysdate, 'YYYYMMDD') || 'T' ||
                 to_char(sysdate, 'HH24MI') Extraction_date,
                 0 Ordered_Qty,
                 0 Sale_Qty,
                 case when nvl(PD.Cancel_qty, 0) > nvl(pd.so_qty,0) then
                   nvl(pd.so_qty,0)
                 else
                   nvl(pd.cancel_qty,0)
                  end as  Cancel_QTY,
                 0 Return_qty,
                 to_char(ph.so_date, 'YYYYMMDD') Demand_Date,                 
                 '' Customer_Code,
                 'N' Use_For_Forcasting,
                 case
                   when Ph.Party_Type in ('D', 'DI') then
                    'N'
                   else
                    ''
                 end as Use_For_Service_Level,
                 'N' Use_For_Classification,
                 '' Stock_demand_date,
                 'Counter Sale' Sales_Type,
                 '' Job_Card_Number,
                 '' Job_Card_Status,
                 PH.Party_Type Party_Type,
                 '' Service_Type,
                 'N' Use_For_Sales_Value_KPI,
                 '' Demand_Stream,
                 '' Direct_Demand,
                 '' Order_date
                  FROM PH_SO             PH,
                       PD_SO             PD,
                       AM_DEALER_LOC     AM,
                       am_company_master ma
                 WHERE PH.DEALER_MAP_CD = PD.DEALER_MAP_CD
                   AND PH.LOC_CD = PD.LOC_CD
                   AND PH.PARENT_GROUP = PD.PARENT_GROUP
                   AND PH.SO_NUM = PD.SO_NUM
                   AND PH.COMP_FA = PD.COMP_FA
                   AND PH.DEALER_MAP_CD = AM.DEALER_MAP_CD
                   AND PH.LOC_CD = AM.LOC_CD
                   AND PH.PARENT_GROUP = AM.PARENT_GROUP
                   and ph.comp_fa = ma.comp_code
                   and ma.parent_group = AM.parent_group
                   and ma.dealer_map_cd = AM.dealer_map_cd
                   and AM.principal_map_cd = 1
                   and TRUNC(PD.canceled_date) >= P_FROM_DATE
                   and TRUNC(PD.canceled_date) < P_TO_DATE
                   and TRUNC(PH.So_Date) >= P_FROM_DATE
                   and TRUNC(PH.So_Date) < P_TO_DATE)
         group by Raw_Transaction_Type,
                  Item_Code,
                  Warehouse_code,
                  Warehouse_Grp_Cd,
                  Sales_Order_Num,
                  Sales_Ord_Line_Num,
                  Extraction_date,
                  Demand_Date,
                  Customer_Code,
                  Use_For_Forcasting,
                  Use_For_Service_Level,
                  Use_For_Classification,
                  Sales_Type,
                  Job_Card_Number,                  
                  Party_Type,
                  Service_Type,
                  Use_For_Sales_Value_KPI,
                  Demand_Stream,
                  Direct_Demand,
                  Order_Date,
                  Stock_demand_date,
                  Prefix
        
        UNION ALL
        Select Raw_Transaction_Type,
               Item_Code,
               Prefix,
               Warehouse_code,
               Warehouse_Grp_Cd,
               Sales_Order_Num,
               Sales_Ord_Line_Num,
               Extraction_date,
               sum(Sale_qty - return_qty) Ordered_quantity,
               sum(Sale_Qty - return_qty) Supplied_quantity,
               Demand_Date,
               Customer_Code,
               Use_For_Forcasting,
               Use_For_Service_Level,
               Use_For_Classification,
               Stock_demand_date,
               Sales_Type,
               Job_Card_Number,
               Job_Card_Status,
               Party_Type,
               Service_Type,
               Use_For_Sales_Value_KPI,
               Demand_Stream,
               Direct_Demand,
               TO_DATE(Order_Date, 'dd-mm-yyyy'),
               SYSDATE Created_date
          from (SELECT /*+Index( ph IDX_PH_ISSUE_DATE,AM XPKAM_DEALER_LOC)+*/
                 'M' Raw_Transaction_Type,
                 PD.PART_NUM Item_Code,
                 '' Prefix,
                 AM.PARENT_GROUP || '-' || AM.Dealer_Map_Cd || '-' ||
                 AM.LOC_CD Warehouse_code,
                 AM.Region_cd || '_' || AM.PARENT_GROUP Warehouse_Grp_Cd,
                 PSO.SO_NUM Sales_Order_Num,
                 AM.PARENT_GROUP || AM.Dealer_Map_Cd || AM.LOC_CD ||
                 PSO.SO_NUM || PD.PART_NUM || to_char(PH.doc_date, 'YYMMDD') || 'F' Sales_Ord_Line_Num,
                 to_char(sysdate, 'YYYYMMDD') || 'T' ||
                 to_char(sysdate, 'HH24MI') Extraction_date,
                 0 Ordered_qty,
                 nvl(PD.Bill_Qty, 0) Sale_Qty,
                 0 Cancel_Qty,
                 nvl(pd.ret_qty, 0) Return_qty,
                 to_char(PH.doc_date, 'YYYYMMDD') Demand_Date,
                 '' Customer_Code,
                 case
                   when Ph.Party_Type in ('D', 'DI') then
                    'N'
                   when PM.Catg_Cd = 'AA' and AM.Dealer_Type <> 'TV' and
                        am.dealer_category = 'DDL' then
                    'N'
                   else
                    ''
                 end as Use_For_Forcasting,
                 'N' Use_For_Service_Level,
                 case
                   when Ph.Party_Type in ('D', 'DI') then
                    'N'
                   else
                    ''
                 end as Use_For_Classification,
                 '' Stock_demand_date,
                 'Counter Sale' Sales_Type,
                 '' Job_Card_Number,
                 to_char(pso.so_date, 'YYYYMMDD') Job_Card_Status,
                 PH.Party_Type Party_Type,
                 '' Service_Type,
                 case
                   when Ph.Party_Type in ('D', 'DI') then
                    'N'
                   else
                    ''
                 end as Use_For_Sales_Value_KPI,
                 '' Demand_Stream,
                 '' Direct_Demand,
                 '' Order_date
                  from PH_ISSUE          ph,
                       PD_ISSUE          pd,
                       am_dealer_loc     AM,
                       PM_PART           PM,
                       am_company_master ma,
                       PH_SO             PSO,
                       PD_SO             PDO                
                 where Ph.DOC_TYPE IN ('RSI', 'CSI')
                   AND ph.parent_group = AM.parent_group
                   and ph.dealer_map_cd = AM.dealer_map_cd
                   and ph.loc_cd = AM.loc_cd
                   and ph.comp_fa = ma.comp_code
                   and (ph.doc_date) >= P_FROM_DATE
                   and (ph.doc_date) < P_TO_DATE
                   and pd.parent_group = ph.parent_group
                   and pd.dealer_map_cd = ph.dealer_map_cd
                   and pd.loc_cd = ph.loc_cd
                   and pd.comp_fa = ph.comp_fa
                   and pd.doc_type = ph.doc_type
                   and pd.doc_num = ph.doc_num
                   and pd.srl_num >= 0
                   AND PM.PART_NUM = pd.PART_NUM
                   AND PM.DEALER_MAP_CD = 1
                   and ma.parent_group = AM.parent_group
                   and ma.dealer_map_cd = AM.dealer_map_cd
                   and AM.principal_map_cd = 1
                   AND PSO.DEALER_MAP_CD = PDO.DEALER_MAP_CD
                   AND PSO.LOC_CD = PDO.LOC_CD
                   AND PSO.PARENT_GROUP = PDO.PARENT_GROUP
                   AND PSO.SO_NUM = PDO.SO_NUM
                   AND PSO.COMP_FA = PDO.COMP_FA
                   and pdo.part_num = pd.part_num
                   AND PDO.SRL_NUM > 0
                   and pso.parent_group = ph.parent_group
                   and pso.dealer_map_cd = ph.dealer_map_cd
                   and pso.loc_cd = ph.loc_cd
                   and pso.comp_fa = ph.comp_fa
                   and pso.so_num = pd.ref_doc_num)
         group by Raw_Transaction_Type,
                  Item_Code,
                  Warehouse_code,
                  Warehouse_Grp_Cd,
                  Sales_Order_Num,
                  Sales_Ord_Line_Num,
                  Extraction_date,
                  Demand_Date,
                  Customer_Code,
                  Use_For_Forcasting,
                  Use_For_Service_Level,
                  Use_For_Classification,
                  Sales_Type,
                  Job_Card_Number,
                  Job_Card_Status,
                  Party_Type,
                  Service_Type,
                  Use_For_Sales_Value_KPI,
                  Demand_Stream,
                  Direct_Demand,
                  Order_Date,
                  Stock_demand_date,
                  Prefix;
    
      COMMIT;
    END;
  
  END SP_TRAN_DEMAND_CO;

  PROCEDURE SP_TRAN_DEMAND_WORKSHOP(P_FROM_DATE IN DATE, P_TO_DATE IN DATE) IS
  BEGIN
  
    BEGIN
    
      Insert into PH_TRANS_DEMAND_WORKSHOP_POC
        (Raw_Transaction_Type,
         Item_Code,
         Prefix,
         Warehouse_code,
         Warehouse_Grp_Cd,
         Sales_Order_Num,
         Sales_Ord_Line_Num,
         Extraction_date,
         Ordered_quantity,
         Supplied_quantity,
         Demand_Date,
         Customer_Code,
         Use_For_Forcasting,
         Use_For_Service_Level,
         Use_For_Classification,
         Stock_demand_date,
         Sales_Type,
         Job_Card_Number,
         Job_Card_Status,
         Party_Type,
         Service_Type,
         Use_For_Sales_Value_KPI,
         Demand_Stream,
         Direct_Demand,
         Order_Date,
         Created_Date)
        Select Raw_Transaction_Type,
               Item_Code,
               Prefix,
               Warehouse_code,
               Warehouse_Grp_Cd,
               Sales_Order_Num,
               Sales_Ord_Line_Num,
               Extraction_date,
               sum(Ordered_Qty - return_qty - Cancel_Qty) Ordered_quantity,
               sum(Sale_Qty - return_qty) Supplied_quantity,
               (Demand_Date),
               Customer_Code,
               Use_For_Forcasting,
               Use_For_Service_Level,
               Use_For_Classification,
               Stock_demand_date,
               Sales_Type,
               Job_Card_Number,
               max(Job_Card_Status),
               Party_Type,
               Service_Type,
               Use_For_Sales_Value_KPI,
               Demand_Stream,
               Direct_Demand,
               TO_DATE(Order_Date, 'dd-mm-yyyy'),
               SYSDATE Created_date
          from (SELECT 'M' Raw_Transaction_Type,
                       VT.PART_NUM Item_Code,
                       '' Prefix,
                       AM.PARENT_GROUP || '-' || AM.Dealer_Map_Cd || '-' ||
                       AM.LOC_CD Warehouse_code,
                       AM.Region_cd || '_' || AM.PARENT_GROUP Warehouse_Grp_Cd,
                       VT.Req_Num Sales_Order_Num,
                       AM.PARENT_GROUP || AM.Dealer_Map_Cd || AM.LOC_CD ||
                       VT.Req_Num || VT.PART_NUM ||
                       to_char(vt.req_date, 'YYMMDD') || 'R' Sales_Ord_Line_Num,
                       to_char(sysdate, 'YYYYMMDD') || 'T' ||
                       to_char(sysdate, 'HH24MI') Extraction_date,
                       nvl(VT.Req_Qty, 0) Ordered_Qty,
                       0 Sale_Qty,
                       0 Cancel_Qty,
                       0 Return_Qty,
                       to_char(vt.req_date, 'YYYYMMDD') Demand_Date,                      
                       '' Customer_Code,
                       'N' Use_For_Forcasting,
                       case
                         when vh.ro_status = 20 then
                          'N'
                         else
                          ''
                       end as Use_For_Service_Level,
                       'N ' Use_For_Classification,
                       '' Stock_demand_date,
                       'Workshop' Sales_Type,
                       Vt.Ro_Num || '_' || vh.ro_status Job_Card_Number,
                       '' Job_Card_Status,
                       'CU' Party_Type,
                       vh.rcateg_cd Service_Type,
                       'N' Use_For_Sales_Value_KPI,
                       '' Demand_Stream,
                       '' Direct_Demand,
                       '' Order_date
                  FROM VT_REQ            VT,
                       VH_RO             vh,
                       AM_DEALER_LOC     AM,
                       am_company_master ma
                 WHERE VH.DEALER_MAP_CD = AM.DEALER_MAP_CD
                   AND VH.LOC_CD = AM.LOC_CD
                   AND VH.PARENT_GROUP = AM.PARENT_GROUP
                   and vh.comp_fa = ma.comp_code
                   and TRUNC(VT.REQ_DATE) >= P_FROM_DATE
                   and TRUNC(VT.REQ_DATE) < P_TO_DATE
                   and vt.dealer_map_cd = vh.dealer_map_cd
                   and vt.parent_group = vh.parent_group
                   and vt.loc_cd = vh.loc_cd
                   and vt.comp_fa = vh.comp_fa
                   and vt.ro_num = vh.ro_num
                   and ma.parent_group = AM.parent_group
                   and ma.dealer_map_cd = AM.dealer_map_cd
                   and AM.principal_map_cd = 1
                
                UNION ALL
                
                SELECT /*+Index( ph IDX_PH_ISSUE_DATE_1, pd PD_ISSUE_PK)+*/
                 'M' Raw_Transaction_Type,
                 PD.PART_NUM Item_Code,
                 '' Prefix,
                 AM.PARENT_GROUP || '-' || AM.Dealer_Map_Cd || '-' ||
                 AM.LOC_CD Warehouse_code,
                 AM.Region_cd || '_' || AM.PARENT_GROUP Warehouse_Grp_Cd,
                 vt.req_num Sales_Order_Num,
                 AM.PARENT_GROUP || AM.Dealer_Map_Cd || AM.LOC_CD ||
                 VT.Req_Num || PD.PART_NUM || to_char(vt.req_date, 'YYMMDD') || 'R' Sales_Ord_Line_Num,
                 to_char(sysdate, 'YYYYMMDD') || 'T' ||
                 to_char(sysdate, 'HH24MI') Extraction_date,
                 0 Ordered_qty,
                 nvl(pd.bill_qty, 0) Sale_Qty,
                 0 Cancel_Qty,
                 nvl(PD.ret_qty, 0) Return_qty,
                 to_char(vt.req_date, 'YYYYMMDD') Demand_Date,                 
                 '' Customer_Code,
                 'N' Use_For_Forcasting,
                 case
                   when vh.ro_status = 20 then
                    'N'
                   else
                    ''
                 end as Use_For_Service_Level,
                 'N ' Use_For_Classification,
                 '' Stock_demand_date,
                 'Workshop' Sales_Type,
                 Vt.Ro_Num || '_' || vh.ro_status Job_Card_Number,
                 to_char(TRUNC(ph.doc_date), 'YYYYMMDD') Job_Card_Status,
                 'CU' Party_Type,
                 vh.rcateg_cd Service_Type,
                 'N' Use_For_Sales_Value_KPI,
                 '' Demand_Stream,
                 '' Direct_Demand,
                 '' Order_date
                  from PH_ISSUE          ph,
                       PD_ISSUE          pd,
                       am_dealer_loc     AM,
                       pm_part           pm,
                       pm_part           pm1,
                       am_company_master ma,
                       VT_REQ            VT,
                       VH_RO             VH
                 where Ph.DOC_TYPE IN ('SRI')
                   and ph.parent_group = AM.parent_group
                   and ph.dealer_map_cd = AM.dealer_map_cd
                   and ph.loc_cd = AM.loc_cd
                   and ph.comp_fa = ma.comp_code
                   and TRUNC(ph.doc_date) >= P_FROM_DATE
                   and TRUNC(ph.doc_date) < P_TO_DATE
                   and TRUNC(VT.REQ_DATE) >= P_FROM_DATE
                   and TRUNC(VT.REQ_DATE) < P_TO_DATE
                   and pd.parent_group = ph.parent_group
                   and pd.dealer_map_cd = ph.dealer_map_cd
                   and pd.loc_cd = ph.loc_cd
                   and pd.comp_fa = ph.comp_fa
                   and pd.doc_type = ph.doc_type
                   and pd.doc_num = ph.doc_num
                   and pd.srl_num >= 0
                   and vt.parent_group = ph.parent_group
                   and vt.dealer_map_cd = ph.dealer_map_cd
                   and vt.loc_cd = ph.loc_cd
                   and vt.comp_fa = ph.comp_fa
                   and vt.part_num = pm.part_num
                   AND PM.PART_NUM = pd.PART_NUM
                   AND PM.DEALER_MAP_CD = 1
                   AND PM1.PART_NUM = vt.part_num
                   AND PM1.DEALER_MAP_CD = 1
                   and vt.req_num = pd.ref_doc_num
                   and ma.parent_group = AM.parent_group
                   and ma.dealer_map_cd = AM.dealer_map_cd
                   and AM.principal_map_cd = 1
                   and vt.dealer_map_cd = vh.dealer_map_cd
                   and vt.parent_group = vh.parent_group
                   and vt.loc_cd = vh.loc_cd
                   and vt.comp_fa = vh.comp_fa
                   and vt.ro_num = vh.ro_num
                
                UNION ALL
                SELECT /*+ USE_INVISIBLE_INDEXES +*/
                 'M' Raw_Transaction_Type,
                 VT.PART_NUM Item_Code,
                 '' Prefix,
                 AM.PARENT_GROUP || '-' || AM.Dealer_Map_Cd || '-' ||
                 AM.LOC_CD Warehouse_code,
                 AM.Region_cd || '_' || AM.PARENT_GROUP Warehouse_Grp_Cd,
                 VT.Req_Num Sales_Order_Num,
                 AM.PARENT_GROUP || AM.Dealer_Map_Cd || AM.LOC_CD ||
                 VT.Req_Num || VT.PART_NUM || to_char(vt.req_date, 'YYMMDD') || 'R' Sales_Ord_Line_Num,
                 to_char(sysdate, 'YYYYMMDD') || 'T' ||
                 to_char(sysdate, 'HH24MI') Extraction_date,
                 0 Ordered_Qty,
                 0 Sale_Qty,
                 nvl((vt.req_qty - vt.iss_qty), 0) Cancel_Qty,
                 0 Return_Qty,
                 to_char(vt.req_date, 'YYYYMMDD') Demand_Date,                 
                 '' Customer_Code,
                 'N' Use_For_Forcasting,
                 case
                   when vh.ro_status = 20 then
                    'N'
                   else
                    ''
                 end as Use_For_Service_Level,
                 'N ' Use_For_Classification,
                 '' Stock_demand_date,
                 'Workshop' Sales_Type,
                 Vt.Ro_Num || '_' || vh.ro_status Job_Card_Number,
                 '' Job_Card_Status,
                 'CU' Party_Type,
                 vh.rcateg_cd Service_Type,
                 'N' Use_For_Sales_Value_KPI,
                 '' Demand_Stream,
                 '' Direct_Demand,
                 '' Order_date
                  FROM VT_REQ            VT,
                       VH_RO             vh,
                       AM_DEALER_LOC     AM,
                       am_company_master ma
                 WHERE VT.DEALER_MAP_CD = AM.DEALER_MAP_CD
                   AND vt.LOC_CD = AM.LOC_CD
                   AND vt.PARENT_GROUP = AM.PARENT_GROUP
                   and vt.comp_fa = ma.comp_code
                   and ma.parent_group = AM.parent_group
                   and ma.dealer_map_cd = AM.dealer_map_cd
                   and AM.principal_map_cd = 1
                   and TRUNC(VT.MODIFIED_DATE) >= P_FROM_DATE
                   and TRUNC(VT.MODIFIED_DATE) < P_TO_DATE
                   and TRUNC(VT.REQ_DATE) >= P_FROM_DATE
                   and TRUNC(VT.REQ_DATE) < P_TO_DATE
                   and vt.req_status = 'C'
                   and vt.dealer_map_cd = vh.dealer_map_cd
                   and vt.parent_group = vh.parent_group
                   and vt.loc_cd = vh.loc_cd
                   and vt.comp_fa = vh.comp_fa
                   and vt.ro_num = vh.ro_num
                
                )
         group by Raw_Transaction_Type,
                  Item_Code,
                  Warehouse_code,
                  Warehouse_Grp_Cd,
                  Sales_Order_Num,
                  Sales_Ord_Line_Num,
                  Extraction_date,
                  Customer_Code,
                  Use_For_Forcasting,
                  Use_For_Service_Level,
                  Use_For_Classification,
                  Sales_Type,
                  Job_Card_Number,                  
                  Party_Type,
                  Service_Type,
                  Use_For_Sales_Value_KPI,
                  Demand_Stream,
                  Direct_Demand,
                  Order_Date,
                  Stock_demand_date,
                  Demand_Date,
                  Prefix
        
        UNION ALL
        Select Raw_Transaction_Type,
               Item_Code,
               Prefix,
               Warehouse_code,
               Warehouse_Grp_Cd,
               Sales_Order_Num,
               Sales_Ord_Line_Num,
               Extraction_date,
               sum(Sale_Qty - return_qty) Ordered_quantity,
               sum(Sale_Qty - return_qty) Supplied_quantity,
               max(Demand_Date),
               Customer_Code,
               Use_For_Forcasting,
               Use_For_Service_Level,
               Use_For_Classification,
               Stock_demand_date,
               Sales_Type,
               Job_Card_Number,
               Job_Card_Status,
               Party_Type,
               Service_Type,
               Use_For_Sales_Value_KPI,
               Demand_Stream,
               Direct_Demand,
               TO_DATE(Order_Date, 'dd-mm-yyyy'),
               SYSDATE Created_date
          from (SELECT /*+Index( ph IDX_PH_ISSUE_DATE_1, pd PD_ISSUE_PK)+*/
                 'M' Raw_Transaction_Type,
                 vt.part_num Item_Code,
                 '' Prefix,
                 AM.PARENT_GROUP || '-' || AM.Dealer_Map_Cd || '-' ||
                 AM.LOC_CD Warehouse_code,
                 AM.Region_cd || '_' || AM.PARENT_GROUP Warehouse_Grp_Cd,
                 vt.req_num Sales_Order_Num,
                 AM.PARENT_GROUP || AM.Dealer_Map_Cd || AM.LOC_CD ||
                 VT.Req_Num || VT.PART_NUM || to_char(PH.doc_date, 'YYMMDD') || 'F' Sales_Ord_Line_Num,
                 to_char(sysdate, 'YYYYMMDD') || 'T' ||
                 to_char(sysdate, 'HH24MI') Extraction_date,
                 0 Ordered_qty,
                 pd.bill_qty Sale_Qty,
                 0 Cancel_Qty,
                 nvl(PD.ret_qty, 0) Return_qty,                 
                 to_char(PH.doc_date, 'YYYYMMDD') Demand_Date,
                 '' Customer_Code,
                 case
                   when vh.ro_status = 40 then
                    ''
                   else
                    'N'
                 end as Use_For_Forcasting,
                 'N' Use_For_Service_Level,
                 case
                   when vh.ro_status = 40 then
                    ''
                   else
                    'N'
                 end as Use_For_Classification,
                 '' Stock_demand_date,
                 'Workshop' Sales_Type,
                 vt.ro_num || '_' || vh.ro_status Job_Card_Number,
                 to_char(vt.req_date, 'YYYYMMDD') Job_Card_Status,
                 'CU' Party_Type,
                 vh.rcateg_cd Service_Type,
                 '' Use_For_Sales_Value_KPI,
                 '' Demand_Stream,
                 '' Direct_Demand,
                 '' Order_date
                  from PH_ISSUE          ph,
                       PD_ISSUE          pd,
                       am_dealer_loc     AM,
                       pm_part           pm,
                       pm_part           pm1,
                       am_company_master ma,
                       VT_REQ            VT,
                       VH_RO             VH
                 where Ph.DOC_TYPE IN ('SRI')
                   AND ph.parent_group = AM.parent_group
                   and ph.dealer_map_cd = AM.dealer_map_cd
                   and ph.loc_cd = AM.loc_cd
                   and ph.comp_fa = ma.comp_code
                   and TRUNC(ph.doc_date) >= P_FROM_DATE
                   and TRUNC(ph.doc_date) < P_TO_DATE
                   and pd.parent_group = ph.parent_group
                   and pd.dealer_map_cd = ph.dealer_map_cd
                   and pd.loc_cd = ph.loc_cd
                   and pd.comp_fa = ph.comp_fa
                   and pd.doc_type = ph.doc_type
                   and pd.doc_num = ph.doc_num
                   and pd.srl_num >= 0
                   and vt.parent_group = ph.parent_group
                   and vt.dealer_map_cd = ph.dealer_map_cd
                   and vt.loc_cd = ph.loc_cd
                   and vt.comp_fa = ph.comp_fa
                   and vt.part_num = pm.part_num
                   AND PM.PART_NUM = pd.PART_NUM
                   AND PM.DEALER_MAP_CD = 1
                   AND PM1.PART_NUM = vt.part_num
                   AND PM1.DEALER_MAP_CD = 1
                   and vt.req_num = pd.ref_doc_num
                   and ma.parent_group = AM.parent_group
                   and ma.dealer_map_cd = AM.dealer_map_cd
                   and AM.principal_map_cd = 1
                   and vt.dealer_map_cd = vh.dealer_map_cd
                   and vt.parent_group = vh.parent_group
                   and vt.loc_cd = vh.loc_cd
                   and vt.comp_fa = vh.comp_fa
                   and vt.ro_num = vh.ro_num
                
                )
         group by Raw_Transaction_Type,
                  Item_Code,
                  Warehouse_code,
                  Warehouse_Grp_Cd,
                  Sales_Order_Num,
                  Sales_Ord_Line_Num,
                  Extraction_date,
                  Customer_Code,
                  Use_For_Forcasting,
                  Use_For_Service_Level,
                  Use_For_Classification,
                  Sales_Type,
                  Job_Card_Number,
                  Job_Card_Status,
                  Party_Type,
                  Service_Type,
                  Use_For_Sales_Value_KPI,
                  Demand_Stream,
                  Direct_Demand,
                  Order_Date,
                  Stock_demand_date,
                  Prefix;
    
      Commit;
    
    END;
  
  END SP_TRAN_DEMAND_WORKSHOP;

  PROCEDURE SP_TRAN_DEMAND_INDENT(P_FROM_DATE IN DATE, P_TO_DATE IN DATE) IS
  
  BEGIN
  
    Insert into PH_TRANS_DEMAND_INDENT_POC
      (Raw_Transaction_Type,
       Item_Code,
       Prefix,
       Warehouse_code,
       Warehouse_Grp_Cd,
       Sales_Order_Num,
       Sales_Ord_Line_Num,
       Extraction_date,
       Ordered_quantity,
       Supplied_quantity,
       Demand_Date,
       Customer_Code,
       Use_For_Forcasting,
       Use_For_Service_Level,
       Use_For_Classification,
       Stock_demand_date,
       Sales_Type,
       Job_Card_Number,
       Job_Card_Status,
       Party_Type,
       Service_Type,
       Use_For_Sales_Value_KPI,
       Demand_Stream,
       Direct_Demand,
       Order_Date,
       Created_Date)
      Select Raw_Transaction_Type,
             Item_Code,
             Prefix,
             Warehouse_code,
             Warehouse_Grp_Cd,
             Sales_Order_Num,
             Sales_Ord_Line_Num,
             Extraction_date,
             sum(Ordered_Qty - return_qty - Cancel_Qty) Ordered_quantity,
             sum(Sale_Qty - return_qty) Supplied_quantity,
             (Demand_Date),
             Customer_Code,
             Use_For_Forcasting,
             Use_For_Service_Level,
             Use_For_Classification,
             Stock_demand_date,
             Sales_Type,
             Job_Card_Number,
             max(Job_Card_Status),
             Party_Type,
             Service_Type,
             Use_For_Sales_Value_KPI,
             Demand_Stream,
             Direct_Demand,
             TO_DATE(Order_Date, 'dd-mm-yyyy'),
             SYSDATE Created_date
        from (SELECT 'M' Raw_Transaction_Type,
                     PD.PART_NUM Item_Code,
                     '' Prefix,
                     AM.PARENT_GROUP || '-' || AM.Dealer_Map_Cd || '-' ||
                     AM.LOC_CD Warehouse_code,
                     AM.Region_cd || '_' || AM.PARENT_GROUP Warehouse_Grp_Cd,
                     PD.INDENT_NUM Sales_Order_Num,
                     AM.PARENT_GROUP || AM.Dealer_Map_Cd || AM.LOC_CD ||
                     PD.INDENT_NUM || PD.PART_NUM ||
                     to_char(PH.Indent_date, 'YYMMDD') || 'R' Sales_Ord_Line_Num,
                     to_char(sysdate, 'YYYYMMDD') || 'T' ||
                     to_char(sysdate, 'HH24MI') Extraction_date,
                     PD.Indent_Qty Ordered_Qty,
                     0 Sale_Qty,
                     0 Cancel_Qty,
                     0 Return_Qty,
                     to_char(PH.Indent_date, 'YYYYMMDD') Demand_Date,
                     '' Customer_Code,
                     'N' Use_For_Forcasting,
                     'N' Use_For_Service_Level,
                     'N' Use_For_Classification,
                     '' Stock_demand_date,
                     'Indent' Sales_Type,
                     '' Job_Card_Number,
                     '' Job_Card_Status,
                     '' Party_Type,
                     '' Service_Type,
                     'N' Use_For_Sales_Value_KPI,
                     '' Demand_Stream,
                     '' Direct_Demand,
                     '' Order_date
                FROM PH_INDENT         PH,
                     PD_INDENT         PD,
                     AM_DEALER_LOC     AM,
                     am_company_master ma 
               WHERE PH.DEALER_MAP_CD = PD.DEALER_MAP_CD
                 AND PH.LOC_CD = PD.LOC_CD
                 AND PH.PARENT_GROUP = PD.PARENT_GROUP
                 AND PH.Indent_Num = PD.Indent_Num
                 AND PH.COMP_FA = PD.COMP_FA
                 AND PH.Indent_to = AM.LOC_CD
                 AND PH.To_Dealer = AM.DEALER_MAP_CD
                 AND PH.PARENT_GROUP = AM.PARENT_GROUP
                 and ma.parent_group = AM.parent_group
                 and ma.dealer_map_cd = AM.dealer_map_cd
                 and AM.principal_map_cd = 1
                 and (PH.Indent_date) >= P_FROM_DATE
                 and (PH.Indent_date) < P_TO_DATE
              
              UNION ALL
              select distinct /*+Index( ph IDX_PH_ISSUE_DATE,AM XPKAM_DEALER_LOC)+*/ 'M' Raw_Transaction_Type,
                              pdo.part_num Item_Code,
                              '' Prefix,
                              AM.PARENT_GROUP || '-' || AM.Dealer_Map_Cd || '-' ||
                              AM.LOC_CD Warehouse_code,
                              AM.Region_cd || '_' || AM.PARENT_GROUP Warehouse_Grp_Cd,
                              PSO.Indent_num Sales_Order_Num,
                              AM.PARENT_GROUP || AM.Dealer_Map_Cd ||
                              AM.LOC_CD || PSO.INDENT_NUM || PDO.PART_NUM ||
                              to_char(PSO.Indent_date, 'YYMMDD') || 'R' Sales_Ord_Line_Num,
                              to_char(sysdate, 'YYYYMMDD') || 'T' ||
                              to_char(sysdate, 'HH24MI') Extraction_date,
                              0 Ordered_Qty,
                              (pd.bill_qty) Sale_Qty,
                              0 Cancel_Qty,
                              0 Return_qty,
                              to_char(PSO.Indent_date, 'YYYYMMDD') Demand_Date,
                              '' Customer_Code,
                              'N' Use_For_Forcasting,
                              'N' Use_For_Service_Level,
                              'N' Use_For_Classification,
                              '' Stock_demand_date,
                              'Indent' Sales_Type,
                              '' Job_Card_Number,
                              to_char(ph.doc_date, 'YYYYMMDD') Job_Card_Status,
                              PH.Party_Type Party_Type,
                              '' Service_Type,
                              'N' Use_For_Sales_Value_KPI,
                              '' Demand_Stream,
                              '' Direct_Demand,
                              '' Order_date
                from PH_ISSUE          ph,
                     PD_ISSUE          pd,
                     ph_pick_list      ppl,
                     am_dealer_loc     am,
                     am_company_master ma,
                     PH_INDENT         pso,
                     PD_INDENT         pdo,
                     pm_part           pm,
                     pm_part           pm1
               where Ph.DOC_TYPE IN ('SFI')
                 AND ph.parent_group = AM.parent_group
                 and ph.dealer_map_cd = AM.dealer_map_cd
                 and ph.loc_cd = AM.loc_cd
                 and ph.comp_fa = ma.comp_code
                 and (ph.doc_date) >= P_FROM_DATE
                 and (ph.doc_date) < P_TO_DATE
                 and (PSO.Indent_date) >= P_FROM_DATE
                 and (PSO.Indent_date) < P_TO_DATE                   
                 and pd.parent_group = ph.parent_group
                 and pd.dealer_map_cd = ph.dealer_map_cd
                 and pd.loc_cd = ph.loc_cd
                 and pd.comp_fa = ph.comp_fa
                 and pd.doc_type = ph.doc_type
                 and pd.doc_num = ph.doc_num
                 and pd.srl_num >= 0
                 and ma.parent_group = AM.parent_group
                 and ma.dealer_map_cd = AM.dealer_map_cd
                 and AM.principal_map_cd = 1
                 and ppl.parent_group = ph.parent_group
                 and ppl.dealer_map_cd = ph.dealer_map_cd
                 and ppl.loc_cd = ph.loc_cd
                 and ppl.comp_fa = ph.comp_fa
                 and ppl.pick_num = ph.ref_doc_num
                 and pso.parent_group = ppl.parent_group
                 and pso.to_dealer = ppl.dealer_map_cd
                 and pso.indent_to = ppl.loc_cd
                 and pso.comp_fa = ppl.comp_fa
                 and pso.indent_num = ppl.ref_doc_num
                 AND PSO.Dealer_Map_Cd = PDO.DEALER_MAP_CD
                 AND PSO.Loc_Cd = PDO.LOC_CD
                 AND PSO.PARENT_GROUP = PDO.PARENT_GROUP
                 and pso.indent_num = pdo.indent_num
                 and ph.to_loc_cd = pso.loc_cd 
                 AND PM.PART_NUM = pd.PART_NUM
                 AND PM.DEALER_MAP_CD = 1
                 AND PM1.PART_NUM = pdo.PART_NUM
                 AND PM1.DEALER_MAP_CD = 1
                 and pm.root_part_num = pm1.root_part_num
                 and pso.comp_fa = pdo.comp_fa
              
              UNION ALL
              
              SELECT 'M' Raw_Transaction_Type,
                     PD.PART_NUM Item_Code,
                     '' Prefix,
                     AM.PARENT_GROUP || '-' || AM.Dealer_Map_Cd || '-' ||
                     AM.LOC_CD Warehouse_code,
                     AM.Region_cd || '_' || AM.PARENT_GROUP Warehouse_Grp_Cd,
                     PD.Indent_num Sales_Order_Num,
                     AM.PARENT_GROUP || AM.Dealer_Map_Cd || AM.LOC_CD ||
                     PD.INDENT_NUM || PD.PART_NUM ||
                     to_char(PH.Indent_date, 'YYMMDD') || 'R' Sales_Ord_Line_Num,
                     to_char(sysdate, 'YYYYMMDD') || 'T' ||
                     to_char(sysdate, 'HH24MI') Extraction_date,
                     0 Ordered_Qty,
                     0 Sale_Qty,
                     case when PD.Cancel_qty>pd.indent_qty then
                       pd.indent_qty
                       else
                         pd.cancel_qty
                      end as Cancel_QTY,
                     0 Return_qty,
                     to_char(PH.INDENT_DATE, 'YYYYMMDD') Demand_Date,
                     '' Customer_Code,
                     'N' Use_For_Forcasting,
                     'N' Use_For_Service_Level,
                     'N' Use_For_Classification,
                     '' Stock_demand_date,
                     'Indent' Sales_Type,
                     '' Job_Card_Number,
                     '' Job_Card_Status,
                     '' Party_Type,
                     '' Service_Type,
                     'N' Use_For_Sales_Value_KPI,
                     '' Demand_Stream,
                     '' Direct_Demand,
                     '' Order_date
                FROM PH_INDENT         PH,
                     PD_INDENT         PD,
                     AM_DEALER_LOC     AM,
                     am_company_master ma,
                     PM_PART           PM
               WHERE PH.DEALER_MAP_CD = PD.DEALER_MAP_CD
                 AND PH.LOC_CD = PD.LOC_CD
                 AND PH.PARENT_GROUP = PD.PARENT_GROUP
                 AND PH.indent_num = PD.Indent_Num
                 AND PH.COMP_FA = PD.COMP_FA
                 and PM.Part_Num = PD.PART_NUM
                 AND PH.Indent_to = AM.LOC_CD
                 AND PH.To_Dealer = AM.DEALER_MAP_CD
                 AND PH.PARENT_GROUP = AM.PARENT_GROUP
                 and ma.parent_group = AM.parent_group
                 and ma.dealer_map_cd = AM.dealer_map_cd
                 and AM.principal_map_cd = 1
                 AND PM.PART_NUM = pd.PART_NUM
                 AND PM.DEALER_MAP_CD = 1                    
                 and TRUNC(PD.canceled_date) >= P_FROM_DATE
                 and TRUNC(PD.canceled_date) < P_TO_DATE
                 and (PH.Indent_date) >= P_FROM_DATE
                 and (PH.Indent_date) < P_TO_DATE
                 AND pd.cancel_yn = 'Y'
              
              )
       group by Raw_Transaction_Type,
                Item_Code,
                Warehouse_code,
                Warehouse_Grp_Cd,
                Sales_Order_Num,
                Sales_Ord_Line_Num,
                Extraction_date,
                Customer_Code,
                Use_For_Forcasting,
                Use_For_Service_Level,
                Use_For_Classification,
                Sales_Type,
                Job_Card_Number,               
                Party_Type,
                Service_Type,
                Use_For_Sales_Value_KPI,
                Demand_Stream,
                Direct_Demand,
                Order_Date,
                Stock_demand_date,
                Demand_Date,
                Prefix;
    COMMIT;
  
  END SP_TRAN_DEMAND_INDENT;

  PROCEDURE SP_TRAN_DEMAND_SUMMARY IS
  
  BEGIN
  
    Insert into PH_TRANS_DEMAND_SUMMARY
      (Raw_Transaction_Type,
       Item_Code,
       Prefix,
       Warehouse_code,
       Warehouse_Grp_Cd,
       Sales_Order_Num,
       Sales_Ord_Line_Num,
       Extraction_date,
       Ordered_quantity,
       Supplied_quantity,
       Demand_Date,
       Customer_Code,
       Use_For_Forcasting,
       Use_For_Service_Level,
       Use_For_Classification,
       Stock_demand_date,
       Sales_Type_ft1,
       Job_Card_Number_ft2,
       Date_ft3,
       Party_Type_ft4,
       Service_Type_ft5,
       Use_For_Sales_Value_KPI,
       Demand_Stream,
       Direct_Demand,
       Order_Date,
       Created_date)
      Select Raw_Transaction_Type,
             Item_Code,
             Prefix,
             Warehouse_code,
             Warehouse_Grp_Cd,
             Sales_Order_Num,
             Sales_Ord_Line_Num,
             Extraction_date,
             ceil(Ordered_quantity),
             ceil(Supplied_quantity),
             Demand_Date,
             Customer_Code,
             Use_For_Forcasting,
             Use_For_Service_Level,
             Use_For_Classification,
             Stock_demand_date,
             Sales_Type,
             Job_Card_Number,
             Job_Card_Status,
             Party_Type,
             Service_Type,
             Use_For_Sales_Value_KPI,
             Demand_Stream,
             Direct_Demand,
             Order_Date,
             Created_Date
        from (Select Raw_Transaction_Type,
                     Item_Code,
                     Prefix,
                     Warehouse_code,
                     Warehouse_Grp_Cd,
                     Sales_Order_Num,
                     Sales_Ord_Line_Num,
                     Extraction_date,
                     Ordered_quantity,
                     Supplied_quantity,
                     Demand_Date,
                     Customer_Code,
                     Use_For_Forcasting,
                     Use_For_Service_Level,
                     Use_For_Classification,
                     Stock_demand_date,
                     Sales_Type,
                     Job_Card_Number,
                     Job_Card_Status,
                     Party_Type,
                     Service_Type,
                     Use_For_Sales_Value_KPI,
                     Demand_Stream,
                     Direct_Demand,
                     to_char(Order_Date, 'YYYYMMDD') Order_Date,
                     co.created_date
                from ph_trans_demand_co_poc co              
              UNION ALL
              Select Raw_Transaction_Type,
                     Item_Code,
                     Prefix,
                     Warehouse_code,
                     Warehouse_Grp_Cd,
                     Sales_Order_Num,
                     Sales_Ord_Line_Num,
                     Extraction_date,
                     Ordered_quantity,
                     Supplied_quantity,
                     Demand_Date,
                     Customer_Code,
                     Use_For_Forcasting,
                     Use_For_Service_Level,
                     Use_For_Classification,
                     Stock_demand_date,
                     Sales_Type,
                     Job_Card_Number,
                     Job_Card_Status,
                     Party_Type,
                     Service_Type,
                     Use_For_Sales_Value_KPI,
                     Demand_Stream,
                     Direct_Demand,
                     to_char(Order_Date, 'YYYYMMDD') Order_Date,
                     wp.created_date
                from PH_TRANS_DEMAND_WORKSHOP_POC wp            
              UNION all
              Select Raw_Transaction_Type,
                     Item_Code,
                     Prefix,
                     Warehouse_code,
                     Warehouse_Grp_Cd,
                     Sales_Order_Num,
                     Sales_Ord_Line_Num,
                     Extraction_date,
                     Ordered_quantity,
                     Supplied_quantity,
                     Demand_Date,
                     Customer_Code,
                     Use_For_Forcasting,
                     Use_For_Service_Level,
                     Use_For_Classification,
                     Stock_demand_date,
                     Sales_Type,
                     Job_Card_Number,
                     Job_Card_Status,
                     Party_Type,
                     Service_Type,
                     Use_For_Sales_Value_KPI,
                     Demand_Stream,
                     Direct_Demand,
                     to_char(Order_Date, 'YYYYMMDD') Order_Date,
                     pi.created_date
                from ph_trans_demand_indent_POC pi);    
  
    Commit;
  
  END SP_TRAN_DEMAND_SUMMARY;

  PROCEDURE SP_CHD_PO_3YRS_NOT_INDT(P_FROM_DATE IN DATE,
                                    P_TO_DATE   IN DATE,
                                    P_ERR_CD    OUT NUMBER,
                                    P_ERR_MSG   OUT VARCHAR2) IS
  
  BEGIN
    INSERT INTO PO_PLANNING_TOOL
      (ROW_TRANS_TYPE,
       ITEM_CODE,
       WAREHOUSE_CODE,
       WAREHOUSE_GROUP_CODE,
       ORDER_NUM,
       ORDER_LINE_NUM,
       EXTRACTION_DATE,
       CREATED_DATE,
       SUPP_CODE,
       ORDER_QTY,
       ORDER_DATE,
       RECVD_QTY,
       RECVD_DATE)   
    
      SELECT 'M',
             T3.PART_NUM,
             T3.WAREHOUSE_CODE,
             T3.WAREHOUSE_GROUP_CODE,
             DECODE(T3.indent_num, 'NA', T3.REC_DOC_NUM, NULL), 
             T3.WAREHOUSE_CODE || '-' || T3.PART_NUM || '-' ||
             DECODE(T3.indent_num, 'NA', T3.REC_DOC_NUM, NULL), 
             TO_CHAR(SYSDATE, 'YYYYMMDD') || 'T' ||
             TO_CHAR(SYSDATE, 'HH24MI'),
             SYSDATE, 
             T3.from_loc_cd,
             ceil(T3.RECD_QTY), 
             TO_CHAR(T3.MRN_DATE, 'YYYYMMDD') INDENT_DATE,             
             ceil(T3.RECD_QTY) "MRN_QTY",
             TO_CHAR(T3.MRN_DATE, 'YYYYMMDD') "MRN_DATE"      
        FROM (select AM.PARENT_GROUP || '-' || AM.DEALER_MAP_CD || '-' ||
                     AM.LOC_CD WAREHOUSE_CODE,
                     AM.REGION_CD || '_' || AM.PARENT_GROUP WAREHOUSE_GROUP_CODE,
                     PM.ROOT_PART_NUM,
                     pd.part_num,
                     PD.RECD_QTY,
                     PD.REC_DOC_SRL,
                     PH.REC_DOC_TYPE,
                     PH.PARENT_GROUP,
                     PH.COMP_FA,
                     PH.DEALER_MAP_CD,
                     ph.from_loc_cd,
                     ph.loc_cd,
                     PH.REC_DOC_NUM,
                     TRUNC(PH.REC_DOC_DATE) MRN_DATE,
                     PH.INVOICE_NUM,
                     
                     NVL((select PPL.REF_DOC_NUM
                           from PH_PICK_LIST PPL
                          where PPL.PICK_NUM =
                                NVL((select ph1.ref_doc_num
                                      from PH_ISSUE ph1
                                     where ph1.doc_num = PH.INVOICE_NUM
                                       and PH1.parent_group =
                                           PH.PARENT_GROUP
                                       AND PH1.DEALER_MAP_CD =
                                           PH.DEALER_MAP_CD
                                       AND PH1.LOC_CD = PH.FROM_LOC_CD
                                       AND PH1.COMP_FA = PH.COMP_FA
                                       and ph1.doc_date >= '01-JAN-2019'),
                                    'NA')
                            and PPL.parent_group = PH.PARENT_GROUP
                            AND PPL.DEALER_MAP_CD = PH.DEALER_MAP_CD
                            AND PPL.LOC_CD = PH.FROM_LOC_CD
                            AND PPL.COMP_FA = PH.COMP_FA),
                         'NA') indent_num
              
                from ph_receipts       ph,
                     Pd_Receipts       PD,
                     AM_DEALER_LOC     AM,
                     am_company_master ma,
                     pm_part           PM
               where PH.DEALER_MAP_CD = PD.DEALER_MAP_CD
                 AND PH.REC_DOC_TYPE = PD.REC_DOC_TYPE
                 AND PH.REC_DOC_NUM = PD.REC_DOC_NUM
                 AND PH.COMP_FA = PD.COMP_FA
                 AND PH.LOC_CD = PD.LOC_CD
                 AND PH.PARENT_GROUP = PD.PARENT_GROUP
                 AND PD.REC_DOC_SRL > 0
                 AND PH.DEALER_MAP_CD = AM.DEALER_MAP_CD
                 AND PH.LOC_CD = AM.LOC_CD
                 AND PH.PARENT_GROUP = AM.PARENT_GROUP
                 AND PH.Comp_Fa = MA.Comp_Code
                 and ma.parent_group = AM.parent_group
                 and ma.dealer_map_cd = AM.dealer_map_cd
                 and AM.principal_map_cd = 1
                 AND PH.REC_DOC_TYPE IN ('SFR')
                 AND PM.PART_NUM = PD.PART_NUM
                 AND PM.DEALER_MAP_CD = 1
                 AND TRUNC(PH.Rec_Doc_Date) >= P_FROM_DATE
                 AND TRUNC(PH.Rec_Doc_Date) < P_TO_DATE) t3
       where t3.indent_num = 'NA';
  commit;
  EXCEPTION
    WHEN OTHERS THEN
      p_err_cd  := 1;
      p_err_msg := p_err_msg || SQLERRM;
    
  END SP_CHD_PO_3YRS_NOT_INDT;

  PROCEDURE SP_CHD_PO_3YRS_INDT(P_FROM_DATE IN DATE,
                                P_TO_DATE   IN DATE,
                                P_ERR_CD    OUT NUMBER,
                                P_ERR_MSG   OUT VARCHAR2) IS
  
  BEGIN
  
    INSERT INTO PO_PLANNING_TOOL
      (ROW_TRANS_TYPE,
       ITEM_CODE,
       WAREHOUSE_CODE,
       WAREHOUSE_GROUP_CODE,
       ORDER_NUM,
       ORDER_LINE_NUM,
       EXTRACTION_DATE,
       CREATED_DATE,
       SUPP_CODE,
       ORDER_QTY,
       ORDER_DATE,
       RECVD_QTY,
       RECVD_DATE)   
      select 'M',
             T2.ORDERED_PART_NUM,
             T2.WAREHOUSE_CODE,
             T2.WAREHOUSE_GROUP_CODE,
             T2.indent_num,
             T2.WAREHOUSE_CODE || '-' || T2.ORDERED_PART_NUM || '-' ||
             T2.indent_num,
             TO_CHAR(SYSDATE, 'YYYYMMDD') || 'T' ||
             TO_CHAR(SYSDATE, 'HH24MI') EXTRACTION_DATE,
             SYSDATE CREATED_DATE,
             T2.indent_to,
             ceil(T2.indent_qty),
             TO_CHAR(T2.INDENT_DATE, 'YYYYMMDD'),
             ceil(SUM(T2.MRN_QTY)) "MRN_QTY",
             TO_CHAR(max(T2.MRN_DATE), 'YYYYMMDD') "MRN_DATE"
        FROM (SELECT pd.part_num ORDERED_PART_NUM,
                     PP.ROOT_PART_NUM,
                     AM.PARENT_GROUP || '-' || AM.DEALER_MAP_CD || '-' ||
                     AM.LOC_CD WAREHOUSE_CODE,
                     AM.REGION_CD || '_' || AM.PARENT_GROUP WAREHOUSE_GROUP_CODE,
                     pd.indent_num,
                     ph.indent_to,
                     pd.indent_qty,
                     TRUNC(PH.INDENT_DATE) "INDENT_DATE",
                     NULL MRN_NUM,
                     0 MRN_QTY,
                     NULL MRN_DATE
                FROM PH_INDENT         PH,
                     PD_INDENT         PD,
                     AM_DEALER_LOC     AM,
                     am_company_master ma,
                     PM_PART           PP
              
               where PH.Indent_date >= P_FROM_DATE
                 AND PH.INDENT_DATE < P_TO_DATE
                 AND PH.DEALER_MAP_CD = PD.DEALER_MAP_CD
                 AND PH.LOC_CD = PD.LOC_CD
                 AND PH.PARENT_GROUP = PD.PARENT_GROUP
                 AND PH.INDENT_NUM = PD.INDENT_NUM
                 AND PH.COMP_FA = PD.COMP_FA
                 AND PD.STI_SRL > 0
                 AND PH.INDENT_STATUS NOT IN ('C', 'H')
                 AND PH.DEALER_MAP_CD = AM.DEALER_MAP_CD
                 AND PH.LOC_CD = AM.LOC_CD
                 AND PH.PARENT_GROUP = AM.PARENT_GROUP
                 AND AM.DEALER_MAP_CD = PH.DEALER_MAP_CD
                 AND AM.LOC_CD = PH.LOC_CD
                 AND AM.PRINCIPAL_MAP_CD = 1
                 and ma.parent_group = AM.parent_group
                 and ma.dealer_map_cd = AM.dealer_map_cd
                 AND PP.PART_NUM = PD.PART_NUM
                 AND PP.DEALER_MAP_CD = 1
              
              UNION ALL             
              SELECT PDT.PART_NUM ORDERED_PART_NUM,
                     PP.ROOT_PART_NUM,
                     T1.WAREHOUSE_CODE,
                     T1.WAREHOUSE_GROUP_CODE,
                     t1.indent_num,
                     t1.from_loc_cd INDENT_TO,
                     PDT.INDENT_QTY,
                     trunc(pdt.created_date) INDENT_DATE,
                     T1.REC_DOC_NUM MRN_NUM,
                     T1.RECD_QTY MRN_QTY,
                     T1.MRN_DATE
                FROM PD_INDENT PDT,
                     PM_PART PP,
                     (select AM.PARENT_GROUP || '-' || AM.DEALER_MAP_CD || '-' ||
                             AM.LOC_CD WAREHOUSE_CODE,
                             AM.REGION_CD || '_' || AM.PARENT_GROUP WAREHOUSE_GROUP_CODE,
                             PM.ROOT_PART_NUM,
                             PD.RECD_QTY,
                             PH.REC_DOC_TYPE,
                             PH.PARENT_GROUP,
                             PH.COMP_FA,
                             PH.DEALER_MAP_CD,
                             ph.from_loc_cd,
                             ph.loc_cd,
                             PH.REC_DOC_NUM,
                             TRUNC(PH.REC_DOC_DATE) MRN_DATE,
                             PH.INVOICE_NUM,
                             
                             NVL((select PPL.REF_DOC_NUM
                                   from PH_PICK_LIST PPL
                                  where PPL.PICK_NUM =
                                        NVL((select ph1.ref_doc_num
                                              from PH_ISSUE ph1
                                             where ph1.doc_num =
                                                   PH.INVOICE_NUM
                                               and PH1.parent_group =
                                                   PH.PARENT_GROUP
                                               AND PH1.DEALER_MAP_CD =
                                                   PH.DEALER_MAP_CD
                                               AND PH1.LOC_CD =
                                                   PH.FROM_LOC_CD
                                               AND PH1.COMP_FA = PH.COMP_FA
                                               and ph1.doc_date >=
                                                   '01-JAN-2019'),
                                            'NA')
                                    and PPL.parent_group = PH.PARENT_GROUP
                                    AND PPL.DEALER_MAP_CD = PH.DEALER_MAP_CD
                                    AND PPL.LOC_CD = PH.FROM_LOC_CD
                                    AND PPL.COMP_FA = PH.COMP_FA),
                                 'NA') indent_num
                      
                        from ph_receipts       ph,
                             Pd_Receipts       PD,
                             AM_DEALER_LOC     AM,
                             am_company_master ma,
                             pm_part           PM
                       where PH.DEALER_MAP_CD = PD.DEALER_MAP_CD
                         AND PH.REC_DOC_TYPE = PD.REC_DOC_TYPE
                         AND PH.REC_DOC_NUM = PD.REC_DOC_NUM
                         AND PH.COMP_FA = PD.COMP_FA
                         AND PH.LOC_CD = PD.LOC_CD
                         AND PH.PARENT_GROUP = PD.PARENT_GROUP
                         AND PD.REC_DOC_SRL > 0
                         AND PH.DEALER_MAP_CD = AM.DEALER_MAP_CD
                         AND PH.LOC_CD = AM.LOC_CD
                         AND PH.PARENT_GROUP = AM.PARENT_GROUP
                         AND PH.Comp_Fa = MA.Comp_Code
                         and ma.parent_group = AM.parent_group
                         and ma.dealer_map_cd = AM.dealer_map_cd
                         and AM.principal_map_cd = 1
                         AND PH.REC_DOC_TYPE IN ('SFR', 'STR', 'SGR')
                         AND PM.PART_NUM = PD.PART_NUM
                         AND PM.DEALER_MAP_CD = 1
                         AND TRUNC(PH.Rec_Doc_Date) >= P_FROM_DATE
                         AND TRUNC(PH.Rec_Doc_Date) < P_TO_DATE) t1
               where PDT.STI_SRL > 0
                 AND PDT.INDENT_NUM = t1.indent_num
                 AND PDT.DEALER_MAP_CD = T1.DEALER_MAP_CD
                 AND PDT.LOC_CD = T1.LOC_CD
                 AND PDT.PARENT_GROUP = T1.PARENT_GROUP
                 AND PDT.COMP_FA = T1.COMP_FA
                 and PP.PART_NUM = PDT.PART_NUM
                 AND PP.DEALER_MAP_CD = 1
                 AND PP.ROOT_PART_NUM = T1.ROOT_PART_NUM) T2
      
       GROUP BY T2.ORDERED_PART_NUM,
                T2.WAREHOUSE_CODE,
                T2.WAREHOUSE_GROUP_CODE,
                T2.indent_num,
                T2.indent_to,
                T2.indent_qty,
                T2.INDENT_DATE;
  
  EXCEPTION
    WHEN OTHERS THEN
      p_err_cd  := 1;
      p_err_msg := p_err_msg || SQLERRM;
  END SP_CHD_PO_3YRS_INDT;

  PROCEDURE SP_CHD_PO_3YRS_CO(P_FROM_DATE IN DATE,
                              P_TO_DATE   IN DATE,
                              P_ERR_CD    OUT NUMBER,
                              P_ERR_MSG   OUT VARCHAR2) IS
  
  BEGIN
  
    INSERT INTO PO_PLANNING_TOOL
      (ROW_TRANS_TYPE,
       ITEM_CODE,
       WAREHOUSE_CODE,
       WAREHOUSE_GROUP_CODE,
       ORDER_NUM,
       ORDER_LINE_NUM,
       EXTRACTION_DATE,
       CREATED_DATE,
       SUPP_CODE,
       ORDER_QTY,
       ORDER_DATE,
       RECVD_QTY,
       RECVD_DATE)    
      SELECT 'M',
             T1.ORDERED_PART_NUM,
             T1.WAREHOUSE_CODE,
             T1.WAREHOUSE_GROUP_CODE,
             T1.INDENT_NUM,
             T1.WAREHOUSE_CODE || '-' || T1.ORDERED_PART_NUM || '-' ||
             T1.INDENT_NUM,
             TO_CHAR(SYSDATE, 'YYYYMMDD') || 'T' ||
             TO_CHAR(SYSDATE, 'HH24MI') EXTRACTION_DATE,
             SYSDATE CREATED_DATE,
             T1.INDENT_TO,
             ceil(T1.INDENT_QTY),
             T1.INDENT_DATE,
             ceil(DECODE(T1.Mrn_Date, NULL, NULL, T1.Mrn_QTY)) "Mrn_QTY",
             TO_CHAR(T1.Mrn_Date, 'YYYYMMDD')
        FROM (SELECT PD.PART_NUM ORDERED_PART_NUM,
                     AM1.PARENT_GROUP || '-' || AM1.DEALER_MAP_CD || '-' ||
                     AM1.LOC_CD WAREHOUSE_CODE,
                     AM1.REGION_CD || '_' || AM1.PARENT_GROUP WAREHOUSE_GROUP_CODE,
                     pd.so_num "INDENT_NUM",
                     PH.DEALER_MAP_CD || PH.LOC_CD "INDENT_TO",
                     PD.SO_QTY "INDENT_QTY",
                     TO_CHAR(PH.SO_DATE, 'YYYYMMDD') INDENT_DATE,
                     PDS.BILL_QTY Mrn_QTY,
                     (select TRUNC(NVL(pd2.Modified_Date, PD2.CREATED_DATE))
                        from ph_receipts pr1, PD_RECEIPTS pD2
                       where pr1.invoice_num = PDS.DOC_NUM
                         And PR1.parent_group = AM1.PARENT_GROUP
                         AND PR1.DEALER_MAP_CD = AM1.DEALER_MAP_CD
                         AND PR1.LOC_CD = AM1.Loc_cd
                         AND PR1.COMP_FA =
                             (select ac.comp_code
                                from am_company_master ac
                               where dealer_map_cd = am1.dealer_map_cd)
                         AND PR1.Rec_Doc_Date is not null
                         AND PD2.DEALER_MAP_CD = PR1.DEALER_MAP_CD
                         AND PD2.REC_DOC_TYPE = PR1.REC_DOC_TYPE
                         AND PD2.REC_DOC_NUM = PR1.REC_DOC_NUM
                         AND PD2.COMP_FA = PR1.COMP_FA
                         AND PD2.LOC_CD = PR1.LOC_CD
                         AND PD2.REC_DOC_SRL > 0
                         AND PD2.PARENT_GROUP = PR1.PARENT_GROUP
                         AND PD2.PART_NUM = PDS.PART_NUM
                         AND PD2.RECEIVE_STOCK = 'Y'
                         AND PD2.BATCH_NUM = PDS.BATCH) Mrn_Date
                FROM Ph_So             PH,
                     Pd_So             PD,
                     AM_DEALER_LOC     AM1,
                     am_company_master ma,
                     pm_part           pm,
                     pm_part           pm1,
                     PH_ISSUE          phs,
                     PD_ISSUE          pds
              
               WHERE PH.DEALER_MAP_CD = PD.DEALER_MAP_CD
                 AND PH.LOC_CD = PD.LOC_CD
                 AND PH.PARENT_GROUP = PD.PARENT_GROUP
                 AND PH.SO_NUM = PD.SO_NUM
                 AND PH.COMP_FA = PD.COMP_FA
                 AND PD.SRL_NUM > 0                   
                 AND PH.Comp_Fa = MA.Comp_Code
                 and (PH.So_Date) >= P_FROM_DATE
                 and (PH.So_Date) < P_TO_DATE
                 and ma.parent_group = PH.parent_group
                 and ma.dealer_map_cd = PH.dealer_map_cd
                    
                 and ph.party_type in ('D', 'DI')
                 and pd.canceled_date is null
                 AND PM.PART_NUM = pd.PART_NUM
                 AND PM.DEALER_MAP_CD = 1
                    
                 and pds.parent_group = phs.parent_group
                 and pds.dealer_map_cd = phs.dealer_map_cd
                 and pds.loc_cd = phs.loc_cd
                 and pds.comp_fa = phs.comp_fa
                 and pds.doc_type = phs.doc_type
                 and pds.doc_num = phs.doc_num
                 and pds.srl_num >= 0
                 and pds.ref_doc_num = pd.so_num
                 AND PH.PARENT_GROUP = PHS.PARENT_GROUP
                 AND PH.DEALER_MAP_CD = PHS.DEALER_MAP_CD
                 AND PH.LOC_CD = PHS.LOC_CD
                 AND PH.COMP_FA = PHS.COMP_FA
                 AND PM1.PART_NUM = pds.PART_NUM
                 AND PM1.DEALER_MAP_CD = 1
                 and PM.ROOT_PART_NUM = PM1.ROOT_PART_NUM
                 AND PH.PARTY_CD = AM1.DEALER_MAP_CD || AM1.LOC_CD
                 AND AM1.Principal_Map_Cd = 1 --added by pooja on 07-Jun-2023
                 ) T1;
  
    commit;
  EXCEPTION
    WHEN OTHERS THEN
      p_err_cd  := 1;
      p_err_msg := p_err_msg || SQLERRM;
  END SP_CHD_PO_3YRS_CO;

  PROCEDURE SP_MOTHER_CL03_DATA_3YRS(P_FROM_DATE IN DATE,
                                     P_TO_DATE   IN DATE,
                                     P_ERR_CD    OUT NUMBER,
                                     P_ERR_MSG   OUT VARCHAR2) IS
  
  BEGIN
  
    INSERT INTO PO_PLANNING_TOOL ---PO_PLANNING_TOOL_CL03
        (ROW_TRANS_TYPE,
         ITEM_CODE,
         WAREHOUSE_CODE,
         WAREHOUSE_GROUP_CODE,
         ORDER_NUM,
         ORDER_LINE_NUM,
         EXTRACTION_DATE,
         CREATED_DATE,
         SUPP_CODE,
         ORDER_QTY,
         ORDER_DATE,
         RECVD_QTY,
         RECVD_DATE)
        SELECT 'M' ROW_TRANS_TYPE,
               T1.ORDP_ITEM_CODE ORDERED_PART_NUM,
               T1.WAREHOUSE_CODE,
               T1.WAREHOUSE_GROUP_CODE,
               T1.INDENT_NUM,
               T1.ORDE_CONS_ID || '-' || T1.ORDP_ITEM_CODE || '-' ||
               SUBSTR(T1.INDENT_NUM, 1, 28) ORDER_LINE_NUM,
               TO_CHAR(SYSDATE, 'YYYYMMDD') || 'T' ||
               TO_CHAR(SYSDATE, 'HH24MI') EXTRACTION_DATE,
               SYSDATE CREATED_DATE,
               'MSIL' "INDENT_TO",
               ceil(GREATEST((T1.INDENT_QTY -
                             NVL((SELECT SUM(PML1.ORDP_QTY)
                                    FROM OPEN_PO_CL PML1
                                   WHERE PML1.ORDE_REF_NO = T1.INDENT_NUM
                                     AND PML1.ORDE_CONS_ID =
                                         T1.ORDE_CONS_ID
                                     AND PML1.ORDP_ITEM_CODE =
                                         T1.ORDP_ITEM_CODE
                                     AND PML1.STATUS = 'CANCEL_DET'
                                     AND PML1.CREATED_DATE >= '20-DEC-2022'),
                                  0)),
                             0)) INDENT_QTY,
               TO_CHAR(T1.INDENT_DATE, 'YYYYMMDD'),
               ceil(decode(T1.MRN_DATE, null, null, T1.MRN_QTY)),
               TO_CHAR(T1.MRN_DATE, 'YYYYMMDD')
          FROM (SELECT PML.ORDP_ITEM_CODE,
                       AM.PARENT_GROUP || '-' || AM.DEALER_MAP_CD || '-' ||
                       AM.LOC_CD WAREHOUSE_CODE,
                       AM.REGION_CD || '_' || AM.PARENT_GROUP WAREHOUSE_GROUP_CODE,
                       PML.ORDE_REF_NO "INDENT_NUM",
                       PML.ORDP_QTY "INDENT_QTY",
                       PML.ORDE_CONS_ID,
                       TRUNC(PML.ORDE_DATE) "INDENT_DATE",
                       SUM(PML.PACP_QTY) "MRN_QTY",
                       MAX((SELECT NVL(PD.MODIFIED_DATE, PD.CREATED_DATE)
                             FROM ph_receipts PH, PD_RECEIPTS PD
                            WHERE PH.DEALER_MAP_CD = AM.DEALER_MAP_CD
                              AND PH.LOC_CD = AM.LOC_CD
                              AND PH.SUPPLIER_CD = 'MUL'
                              AND regexp_substr(PH.INVOICE_NUM,
                                                '[^-]+',
                                                1,
                                                2) = PML.INVO_NO
                              AND PD.DEALER_MAP_CD = PH.DEALER_MAP_CD
                              AND PD.REC_DOC_TYPE = PH.REC_DOC_TYPE
                              AND PD.REC_DOC_NUM = PH.REC_DOC_NUM
                              AND PD.COMP_FA = PH.COMP_FA
                              AND PD.LOC_CD = PH.LOC_CD
                              AND PD.REC_DOC_SRL > 0
                              AND PD.PARENT_GROUP = PH.PARENT_GROUP
                              AND PD.PART_NUM = PML.INVD_ITEM_CODE
                              AND PD.BATCH_NUM = PML.BATCH_NUM
                              AND PD.RECEIVE_STOCK = 'Y')) "MRN_DATE"

                  FROM OPEN_PO_CL PML, AM_DEALER_LOC AM /*,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                       am_company_master ma*/
                 WHERE PML.STATUS IN ('INVOCE_DET')
                   AND AM.PRINCIPAL_MAP_CD = 1                     
                   AND AM.CONSG_CD = PML.ORDE_CONS_ID
                   AND AM.SPARES_STOCK_YARD_YN = 'Y'
                   AND PML.INVO_DATE < P_TO_DATE 
                   AND PML.INVO_DATE >= P_FROM_DATE                   
                   AND PML.CREATED_DATE >= '20-DEC-2022'               
                 GROUP BY PML.ORDP_ITEM_CODE,
                          AM.PARENT_GROUP,
                          AM.DEALER_MAP_CD,
                          AM.LOC_CD,
                          AM.REGION_CD,
                          PML.ORDE_REF_NO,
                          PML.ORDP_QTY,
                          PML.ORDE_CONS_ID,
                          TRUNC(PML.ORDE_DATE)) T1

        UNION ALL

        SELECT DECODE(PML.STATUS, 'FRESH_ORDER_DET', 'M', 'D') ROW_TRANS_TYPE,
               PML.ORDP_ITEM_CODE ORDERED_PART_NUM,
               AM.PARENT_GROUP || '-' || AM.DEALER_MAP_CD || '-' ||
               AM.LOC_CD WAREHOUSE_CODE,
               AM.REGION_CD || '_' || AM.PARENT_GROUP WAREHOUSE_GROUP_CODE,
               PML.ORDE_NO "INDENT_NUM",
               PML.ORDE_CONS_ID || '-' || PML.ORDP_ITEM_CODE || '-' ||
               SUBSTR(PML.ORDE_NO, 1, 28) ORDER_LINE_NUM,
               TO_CHAR(SYSDATE, 'YYYYMMDD') || 'T' ||
               TO_CHAR(SYSDATE, 'HH24MI') EXTRACTION_DATE,
               SYSDATE CREATED_DATE,
               'MSIL' "INDENT_TO",
               (CASE
                 WHEN PML.STATUS = 'FRESH_ORDER_DET' THEN

                  ceil(GREATEST((PML.ORDP_QTY -
                                NVL((SELECT SUM(PML1.ORDP_QTY)
                                       FROM OPEN_PO_CL PML1
                                      WHERE PML1.ORDE_REF_NO =
                                            PML.ORDE_REF_NO
                                        AND PML1.ORDE_CONS_ID =
                                            PML.ORDE_CONS_ID
                                        AND PML1.ORDP_ITEM_CODE =
                                            PML.ORDP_ITEM_CODE
                                        AND PML1.STATUS = 'CANCEL_DET'
                                        AND PML1.CREATED_DATE >=
                                            '20-DEC-2022'),
                                     0)),
                                0))
                 ELSE
                  ceil(PML.ORDP_QTY)
               END) "INDENT_QTY",
               TO_CHAR(PML.ORDE_DATE, 'YYYYMMDD') "INDENT_DATE",
               NULL "MRN_QTY",
               NULL "MRN_DATE"
          FROM OPEN_PO_CL PML, AM_DEALER_LOC AM
         WHERE PML.STATUS IN
               ('FRESH_ORDER_DET', 'EXPIRY_DET', 'ERROR_BATCH')
           AND PML.INVO_NO IS NULL
           AND AM.PRINCIPAL_MAP_CD = 1
           AND AM.CONSG_CD = PML.ORDE_CONS_ID
           AND AM.SPARES_STOCK_YARD_YN = 'Y'
           AND PML.ORDE_DATE < P_TO_DATE --TRUNC(SYSDATE)
           AND PML.ORDE_DATE >= P_FROM_DATE --TRUNC(ADD_MONTHS(SYSDATE, -3))           
           AND PML.CREATED_DATE >= '20-DEC-2022'
              /* Exclude the invoice items*/
           AND NOT EXISTS
         (SELECT 1
                  FROM OPEN_PO_CL op
                 where op.status in ('INVOCE_DET')
                   AND OP.ORDE_REF_NO = PML.ORDE_REF_NO
                   AND OP.ORDP_ITEM_CODE = PML.ORDP_ITEM_CODE
                   AND OP.ORDE_CONS_ID = PML.ORDE_CONS_ID
                   AND OP.CREATED_DATE >= '20-DEC-2022');

     
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      p_err_cd  := 1;
      p_err_msg := p_err_msg || SQLERRM;
  END;

  PROCEDURE SP_MOTHER_DMS_DATA_3YRS(P_FROM_DATE IN DATE,
                                    P_TO_DATE   IN DATE,
                                    P_ERR_CD    OUT NUMBER,
                                    P_ERR_MSG   OUT VARCHAR2) IS
  
  BEGIN
    INSERT INTO PO_PLANNING_TOOL 
      (ROW_TRANS_TYPE,
       ITEM_CODE,
       WAREHOUSE_CODE,
       WAREHOUSE_GROUP_CODE,
       ORDER_NUM,
       ORDER_LINE_NUM,
       EXTRACTION_DATE,
       CREATED_DATE,
       SUPP_CODE,
       ORDER_QTY,
       ORDER_DATE,
       RECVD_QTY,
       RECVD_DATE)
      SELECT 'M' ROW_TRANS_TYPE,
             pd.part_num ORDERED_PART_NUM,
             AM.PARENT_GROUP || '-' || AM.DEALER_MAP_CD || '-' || AM.LOC_CD WAREHOUSE_CODE,
             AM.REGION_CD || '_' || AM.PARENT_GROUP WAREHOUSE_GROUP_CODE,
             pd.Po_Num "INDENT_NUM",
             AM.PARENT_GROUP || '-' || AM.DEALER_MAP_CD || '-' || AM.LOC_CD || '-' ||
             pd.part_num || '-' || pd.Po_Num ORDER_LINE_NUM,
             TO_CHAR(SYSDATE, 'YYYYMMDD') || 'T' ||
             TO_CHAR(SYSDATE, 'HH24MI') EXTRACTION_DATE,
             SYSDATE CREATED_DATE,
             PH.SUPPLIER_CD "INDENT_TO",            
             ceil(GREATEST((pd.po_qty -
                           NVL((SELECT SUM(PML1.ORDP_QTY)
                                  FROM OPEN_PO_CL PML1
                                 WHERE PML1.ORDE_REF_NO = PH.PO_NUM
                                   AND PML1.ORDE_CONS_ID = AM.CONSG_CD
                                   AND PML1.ORDP_ITEM_CODE = PD.PART_NUM
                                   AND PML1.STATUS = 'CANCEL_DET'
                                   AND PML1.CREATED_DATE >= '09-JAN-2023'),
                                0)),
                           0)) "INDENT_QTY",
             TO_CHAR(PH.PO_DATE, 'YYYYMMDD') "INDENT_DATE",
             0 "MRN_QTY",
             NULL "MRN_DATE"
        FROM PH_PO             ph,
             pd_po             pd,
             AM_DEALER_LOC     AM,
             am_company_master ma,
             PM_PART           PP
       WHERE PH.PARENT_GROUP = PD.PARENT_GROUP
         AND PH.DEALER_MAP_CD = PD.DEALER_MAP_CD
         AND PH.LOC_CD = PD.LOC_CD
         AND PH.PO_NUM = PD.PO_NUM
         AND PH.PO_TYPE = PD.PO_TYPE
         AND PH.COMP_FA = PD.COMP_FA
         AND PH.PO_DATE >= P_FROM_DATE 
         AND PH.PO_DATE < P_TO_DATE 
         AND PD.PO_SRL > 0
         AND PH.SUPPLIER_CD = 'MUL'
         AND PH.PO_STATUS = 'R'
         AND PH.PARENT_GROUP = AM.PARENT_GROUP
         AND AM.DEALER_MAP_CD = PH.DEALER_MAP_CD
         AND AM.LOC_CD = PH.LOC_CD
         AND AM.PRINCIPAL_MAP_CD = 1
         AND AM.SPARES_STOCK_YARD_YN = 'Y'
         and ma.parent_group = AM.parent_group
         and ma.dealer_map_cd = AM.dealer_map_cd
         AND PP.PART_NUM = PD.PART_NUM
         AND PP.DEALER_MAP_CD = 1
         AND NOT EXISTS
       (SELECT 1
                FROM OPEN_PO_CL op
               where op.status in
                     ('EXPIRY_DET', 'ERROR_BATCH', 'INVOCE_DET')
                 AND OP.ORDE_REF_NO = PD.PO_NUM
                 AND OP.ORDP_ITEM_CODE = PD.PART_NUM
                 AND OP.ORDE_CONS_ID = AM.CONSG_CD
                 AND OP.CREATED_DATE >= '09-JAN-2023');
  
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      p_err_cd  := 1;
      p_err_msg := p_err_msg || SQLERRM;
  END;

  PROCEDURE SP_PART_SELECTION AS
  BEGIN    
    insert into pm_part_selection_poc
      (select distinct td.warehouse_code,
                       td.item_code part_num,
                       to_date(td.demand_date, 'YYYY-MM-DD')
         from ph_trans_demand_summary td
        where to_date(td.demand_date, 'YYYY-MM-DD') >=
              (add_months(trunc(sysdate), -3 * 12))
          and to_date(td.demand_date, 'YYYY-MM-DD') < trunc(sysdate)
       UNION ALL
       select distinct po.warehouse_code,
                       po.item_code part_num,
                       to_date(po.order_date, 'YYYY-MM-DD')
         from PO_PLANNING_TOOL po
        where to_date(po.order_date, 'YYYY-MM-DD') >=
              add_months(trunc(sysdate), -3 * 12)
          and to_date(po.order_date, 'YYYY-MM-DD') < trunc(sysdate)
       UNION ALL
       select distinct am.parent_group || '-' || am.dealer_map_cd || '-' ||
                       am.loc_cd warehouse_code,
                       ps.part_num part_num,
                       trunc(sysdate) order_date
         from PM_STOCK ps, am_dealer_loc am
        where 1 = 1
          and am.parent_group = ps.parent_group
          and am.loc_Cd = ps.Loc_Cd
          and am.dealer_map_cd = ps.dealer_map_cd             
          and NVL(am.msil_terminated_date, SYSDATE + 1) > SYSDATE
          and am.principal_map_cd = 1
          AND NVL(NVL(os_stock_qty, 0) + NVL(ls_stock_qty, 0) -
                  (NVL(os_float_qty, 0) + NVL(ls_float_qty, 0) +
                   NVL(alloc_qty, 0) + NVL(ws_resrv_qty, 0)),
                  0) <> 0
       
       UNION ALL
       select distinct am.parent_group || '-' || am.dealer_map_cd || '-' ||
                       am.loc_cd warehouse_code,
                       pm.part_num part_num,
                       trunc(pm.eff_from_date) order_date
         from pm_part pm, am_dealer_loc am, pm_part_var pv
        where 1 = 1
          and am.parent_group = pv.parent_group
          and am.loc_Cd = pv.loc_cd
          and am.dealer_map_cd = pv.dealer_map_cd
          and NVL(am.msil_terminated_date, SYSDATE + 1) > SYSDATE
          and am.principal_map_cd = 1
          and pm.part_num = pv.part_num
          and pm.dealer_map_cd = 1
          and pm.principal_map_cd = 1
          and pm.eff_from_date >= trunc(sysdate - 365)
       
       );
  
    commit;
  END;

  PROCEDURE SP_ITEM_DATA(P_ERR_CD OUT NUMBER, P_ERR_MSG OUT VARCHAR2) IS
  
  BEGIN
  
    INSERT INTO ITEM_PLANNING_TOOL
      (ROW_TRANS_TYPE,
       ITEM_CODE,
       WAREHOUSE_CODE,
       WAREHOUSE_GROUP_CODE,
       EXTRACTION_DATE,
       CREATED_DATE,
       DESCRIPTION_ITEM,
       MASTER_ITEM_CODE,
       PREF_SUPP_CODE,
       ACTIVATION_DATE,
       DEACTIVATION_DATE,
       UNIT_COST,
       UNIT_COST_CURR,
       WEIGHT,
       WEIGHT_UOM,
       VOLUME,
       VOLUME_UOM,
       UOM,
       ITEM_GP_1,
       ITEM_GP_2,
       ROOT_NUM_FT1,
       ISS_IND_FT2,
       CONS_CD_FT3,
       DEL_CD_FT4,
       ACC_CD_FT5,
       MUL_CD_FT6,
       FREE_TEXT_7 
       )
      SELECT DISTINCT T1.ROW_TRANS_TYPE,
                      T1.ITEM_CODE,
                      T1.WAREHOUSE_CODE,
                      T1.WAREHOUSE_GROUP_CODE,
                      T1.EXTRACTION_DATE,
                      T1.CREATED_DATE,
                      T1.DESCRIPTION_ITEM,
                      T1.MASTER_ITEM_CODE,
                      T1.PREFERRED_SUPPLIER_CODE,
                      TO_CHAR(T1.ACTIVATION_DATE, 'YYYYMMDD'),
                      TO_CHAR(T1.DEACTIVATION_DATE, 'YYYYMMDD'),
                      TRUNC(DECODE(T1.UNIT_COST, 0, NULL, T1.UNIT_COST), 2),
                      DECODE(T1.UNIT_COST, NULL, NULL, 0, NULL, 'INR'),
                      ROUND(DECODE(T1.WEIGHT,0,NULL,T1.WEIGHT), 2),
                      DECODE((DECODE(T1.WEIGHT,0,NULL,T1.WEIGHT)), NULL, NULL, 'kg'),
                      DECODE(T1.VOLUME,0,NULL,T1.VOLUME),
                      DECODE(DECODE(T1.VOLUME,0,NULL,T1.VOLUME), NULL, NULL, 'cm3'),
                      T1.UOM,
                      T1.ITEM_GP_1,
                      T1.ITEM_GP_2,
                      T1.FREE_TEXT_1,
                      T1.FREE_TEXT_2,
                      T1.FREE_TEXT_3,
                      T1.FREE_TEXT_4,
                      T1.FREE_TEXT_5,
                      T1.FREE_TEXT_6,
                      T1.FREE_TEXT_7 
        FROM (SELECT /*+ USE_INVISIBLE_INDEXES*/
              /*+ index(pm IDX_PM_PART$1)  index(ppv SYS_C003944)  index(am XPKAM_DEALER_LOC) */
               'M' ROW_TRANS_TYPE,
               PM.PART_NUM ITEM_CODE,
               AM.PARENT_GROUP || '-' || AM.DEALER_MAP_CD || '-' ||
               AM.LOC_CD WAREHOUSE_CODE,
               AM.REGION_CD || '_' || AM.PARENT_GROUP WAREHOUSE_GROUP_CODE, --,
               TO_CHAR(SYSDATE, 'YYYYMMDD') || 'T' ||
               TO_CHAR(SYSDATE, 'HH24MI') EXTRACTION_DATE,
               SYSDATE CREATED_DATE,
               replace(PM.PART_DESC,chr(124),chr(47)) DESCRIPTION_ITEM,
               PM.PART_NUM MASTER_ITEM_CODE,
               CASE
                 WHEN AM.SPARES_STOCK_YARD_YN = 'Y' THEN
                  GET_PART_SOURCE(AM.DEALER_MAP_CD,
                                  AM.PARENT_GROUP,
                                  AM.LOC_CD,
                                  PM.PART_NUM)
                 ELSE
                  (SELECT AM1.PARENT_GROUP || '-' || AM1.DEALER_MAP_CD || '-' ||
                          AM1.LOC_CD
                     FROM AM_DEALER_LOC AM1
                    WHERE AM1.PARENT_GROUP = AM.PARENT_GROUP
                      AND AM1.CONSG_CD = AM.CONSG_CD
                      AND AM1.SPARES_STOCK_YARD_YN = 'Y'
                      AND rownum = 1)
               END PREFERRED_SUPPLIER_CODE,
               nvl(PM.EFF_FROM_DATE,PM.CREATED_DATE) ACTIVATION_DATE,
               PM.CLOSED_DATE DEACTIVATION_DATE,
               PPV.Purchase_Price UNIT_COST,               
               (select (pw.mdim_i_weight / 1000)
                  from PART_WGHT_VOL pw
                 where pw.mdim_item_code = PM.PART_NUM
                   AND PW.MDIM_COMP_CODE =
                       nvl((select /*+ index(ps SYS_C0056006)*/
                         AL.LIST_CODE
                          from pm_part_source ps,
                               am_list        al,
                               am_consignee   cn,
                               AM_DEALER_LOC  AM1
                         where ps.part_num = PM.PART_NUM
                           and ps.serve_flag = 'Y'
                           and al.principal_map_cd = 1
                           and al.list_name = 'RPDC_SOURCE'
                           and al.list_code = ps.part_comp_cd
                           and al.principal_map_cd = ps.principal_map_cd
                           and al.list_code = cn.rpdc_status
                           AND cn.consg_cd = AM1.CONSG_CD
                           AND CN.PRINCIPAL_MAP_CD = AM1.PRINCIPAL_MAP_CD
                           AND AM1.CONSG_CD = AM.CONSG_CD
                           AND AM1.DEALER_MAP_CD = AM.DEALER_MAP_CD
                           AND AM1.PARENT_GROUP = AM.PARENT_GROUP
                           AND AM1.LOC_CD = AM.LOC_CD),'01')) WEIGHT,
               (select (pw.mdim_i_length * pw.mdim_i_width * pw.mdim_i_height)
                  from PART_WGHT_VOL pw
                 where pw.mdim_item_code = PM.PART_NUM
                   AND PW.MDIM_COMP_CODE =
                       nvl((select /*+ index(ps SYS_C0056006)*/
                         AL.LIST_CODE
                          from pm_part_source ps,
                               am_list        al,
                               am_consignee   cn,
                               AM_DEALER_LOC  AM1
                         where ps.part_num = PM.PART_NUM
                           and ps.serve_flag = 'Y'
                           and al.principal_map_cd = 1
                           and al.list_name = 'RPDC_SOURCE'
                           and al.list_code = ps.part_comp_cd
                           and al.principal_map_cd = ps.principal_map_cd
                           and al.list_code = cn.rpdc_status
                           AND cn.consg_cd = AM1.CONSG_CD
                           AND CN.PRINCIPAL_MAP_CD = AM1.PRINCIPAL_MAP_CD
                           AND AM1.CONSG_CD = AM.CONSG_CD
                           AND AM1.DEALER_MAP_CD = AM.DEALER_MAP_CD
                           AND AM1.PARENT_GROUP = AM.PARENT_GROUP
                           AND AM1.LOC_CD = AM.LOC_CD),'01')) VOLUME,
               PM.UOM_CD UOM,
               (SELECT PC.CATG_DESC
                  FROM PM_CATEGORY PC
                 WHERE PC.PRINCIPAL_MAP_CD = 1
                   AND PC.CATG_CD = PM.CATG_CD
                   AND ROWNUM = 1) ITEM_GP_1,
               (select AM1.LIST_DESC
                  from am_list am1
                 where AM1.LIST_NAME = 'PART_TYPE'
                   AND AM1.PRINCIPAL_MAP_CD = 1
                   AND am1.list_code =
                       (select PPG1.GROUP_CD
                          FROM Pm_Part_Group PPG1
                         where PPG1.PART_NUM = PPV.PART_NUM
                           AND PPG1.PRINCIPAL_MAP_CD = 1
                           AND PPG1.CREATED_DATE >=
                               (select MAX(PPG.CREATED_DATE)
                                  FROM Pm_Part_Group PPG
                                 where PPG.PART_NUM = PPV.PART_NUM
                                   AND PPG.PRINCIPAL_MAP_CD = 1))) ITEM_GP_2,
               
               PM.ROOT_PART_NUM FREE_TEXT_1,
               PM.TAG_CD FREE_TEXT_2,
               AM.CONSG_CD FREE_TEXT_3,
               AM.MUL_DEALER_CD FREE_TEXT_4,
               (SELECT AC.ACC_CODE
                  FROM am_acc_dealer_code_map ac
                 WHERE AC.DEALER_CD = AM.Mul_Dealer_Cd
                   AND AC.Outlet_Cd = AM.Outlet_Cd
                   AND AC.LOC_CD = AM.LOC_CD) FREE_TEXT_5,
               (select DECODE(FAC.CUST_ACC_ID,
                              NULL,
                              FAC.CONSIGNEE_CODE,
                              FAC.CUST_ACC_ID)
                  from FIN_ACC_CD FAC
                 WHERE FAC.CONSIGNEE_CODE = AM.CONSG_CD
                   AND ROWNUM = 1) FREE_TEXT_6,
               PPV.LANDED_COST FREE_TEXT_7 
                FROM PM_PART_VAR           PPV,
                     PM_PART               PM,
                     AM_DEALER_LOC         AM,
                     PM_PART_SELECTION_POC PPS
               WHERE PPV.PART_NUM = PM.PART_NUM
                 AND PM.DEALER_MAP_CD = 1
                 AND PPV.DEALER_MAP_CD = AM.DEALER_MAP_CD
                 AND PPV.LOC_CD = AM.LOC_CD
                 AND PPV.PARENT_GROUP = AM.PARENT_GROUP
                 AND AM.PRINCIPAL_MAP_CD = 1
                 AND PM.CATG_CD NOT IN ('A', 'L', 'U', 'UA', 'OIL', 'T')
                 AND PPV.PART_NUM = PPS.PART_NUM
                 AND AM.PARENT_GROUP =
                     regexp_substr(Pps.warehouse_code, '[^-]+', 1, 1)
                 AND AM.DEALER_MAP_CD =
                     regexp_substr(Pps.warehouse_code, '[^-]+', 1, 2)
                 AND AM.LOC_CD =
                     regexp_substr(Pps.warehouse_code, '[^-]+', 1, 3)
             
              ) T1;
    COMMIT;
  
  EXCEPTION
    WHEN OTHERS THEN
      p_err_cd  := 1;
      p_err_msg := p_err_msg || SQLERRM;
  END SP_ITEM_DATA;

  PROCEDURE SP_STOCK_DATA(P_ERR_CD OUT NUMBER, P_ERR_MSG OUT VARCHAR2) IS
  BEGIN
    p_err_cd := 0;
  
    INSERT INTO stock_planning_tool
      (ROW_TRANS_TYPE,
       ITEM_CODE,
       PREFIX,
       WAREHOUSE_CODE,
       WAREHOUSE_GROUP_CODE,
       EXTRACTION_DATE,
       CREATED_DATE,
       CURRENT_STOCK,
       OUTSTAND_ORD_QTY,
       TOTAL_BK_ORD,
       IN_TRANS_STK,
       RESERVED_STK,
       BINNING_STK,
       QUAR_STK_FT1,
       DISP_STK_FT2,
       RESRV_STK_FT3,
       ALL_STK_FT4,
       FREE_TEXT_5)
      SELECT 'M', 
             PM.PART_NUM, 
             NULL, 
             AM.PARENT_GROUP || '-' || AM.DEALER_MAP_CD || '-' || AM.LOC_CD, 
             AM.REGION_CD || '_' || AM.PARENT_GROUP, 
             TO_CHAR(SYSDATE, 'YYYYMMDD') || 'T' ||
             TO_CHAR(SYSDATE, 'HH24MI'), 
             SYSDATE, 
             (ceil(greatest(nvl(SUM(NVL(NVL(os_stock_qty, 0) +
                                        NVL(ls_stock_qty, 0) -
                                        (NVL(os_float_qty, 0) +
                                         NVL(ls_float_qty, 0) +
                                         NVL(alloc_qty, 0) +
                                         NVL(ws_resrv_qty, 0)),
                                        0)),
                                0),
                            0))), 
             NULL, 
             (select (ceil(sum(pt.cbo)))
                from PT_CBO_AOS pt
               where pt.part_num = pm.part_num
                 and pt.dealer_map_cd = am.dealer_map_cd
                 and pt.loc_cd = am.loc_cd
                 AND PT.CBO_DATE = TRUNC(SYSDATE)), 
             NULL, 
             ceil(nvl(PD.PART_QTY, 0) + nvl(ppv.quarantine_qty, 0)), 
             NULL, 
             ceil(nvl(ppv.quarantine_qty, 0)),
             
             ceil(nvl(PD.PART_QTY, 0)), 
             ceil(nvl(ppv.Resrv_Qty, 0)), 
             ceil(nvl(pm.alloc_qty, 0)), 
             NULL 
        FROM PM_STOCK_BIN_LOC PM,
             AM_DEALER_LOC AM,
             PM_PART_VAR PPV,
             PM_PART PPM,
             PM_PART_DISP_STOCK PD,
             (select distinct t1.part_num, t1.warehouse_code
                from pm_part_selection_poc t1) pps
       WHERE 
         PPV.PART_NUM = PPM.PART_NUM
          AND PPM.DEALER_MAP_CD = 1
          AND PM.DEALER_MAP_CD = AM.DEALER_MAP_CD
         AND PM.LOC_CD = AM.LOC_CD
         AND PM.PARENT_GROUP = AM.PARENT_GROUP
         AND AM.PRINCIPAL_MAP_CD = 1
         AND PPV.PART_NUM = PM.PART_NUM
         AND PPV.DEALER_MAP_CD = AM.DEALER_MAP_CD
         AND PPV.LOC_CD = AM.LOC_CD
         AND PPV.PARENT_GROUP = AM.PARENT_GROUP
         AND NVL(PM.ACTIVE_YN, 'N') = 'Y'
         AND NVL(PM.DEFAULT_YN, 'N') = 'Y'
         and PD.PART_NUM(+) = PM.PART_NUM
         AND PD.DEALER_MAP_CD(+) = PM.DEALER_MAP_CD
         AND PD.LOC_CD(+) = PM.LOC_CD
         AND PD.PARENT_GROUP(+) = PM.PARENT_GROUP           
         AND PPV.PART_NUM = PPS.PART_NUM
         AND AM.PARENT_GROUP =
             regexp_substr(Pps.warehouse_code, '[^-]+', 1, 1)
         AND AM.DEALER_MAP_CD =
             regexp_substr(Pps.warehouse_code, '[^-]+', 1, 2)
         AND AM.LOC_CD = regexp_substr(Pps.warehouse_code, '[^-]+', 1, 3)
           AND PPM.CATG_CD NOT IN ('A', 'L', 'U', 'UA', 'OIL', 'T')     
       GROUP BY PM.PART_NUM,
                AM.REGION_CD,
                AM.DEALER_MAP_CD,
                AM.LOC_CD,
                AM.PARENT_GROUP,
                NVL(ppv.quarantine_qty, 0),
                NVL(PD.PART_QTY, 0),
                NVL(ppv.Resrv_Qty, 0),
                NVL(pm.alloc_qty, 0);
  
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      p_err_cd  := 1;
      p_err_msg := p_err_msg || SQLERRM;
  END SP_STOCK_DATA;

  ------------This SP to be used only for incremental data-------
  PROCEDURE SP_SPR_CBO_NEW(P_ERR_CD OUT NUMBER, P_ERR_MSG OUT VARCHAR2) IS
    ld_frm_date DATE := trunc(sysdate - 7);
    ld_to_date  DATE := trunc(sysdate);
  
  BEGIN
    DELETE PT_CBO_AOS WHERE cbo_date < trunc(sysdate - 15);
    COMMIT;
    P_ERR_CD := 0;
  
    BEGIN
      for M_indent in (SELECT ph.to_dealer dealer_map_cd,
                              ph.indent_to loc_cd,
                              pm.part_num,
                              SUM(decode(sign(NVL(pd.indent_qty, 0) -
                                              NVL(pd.issued_qty, 0)),
                                         -1,
                                         0,
                                         NVL(pd.indent_qty, 0) -
                                         NVL(pd.issued_qty, 0))) cbo,
                              Count(ph.indent_num) pend_ord,
                              SYSDATE created_date,
                              'I' cbo_type,
                              trunc(SYSDATE) cbo_date
                         FROM ph_indent     ph,
                              pd_indent     pd,
                              pm_part       pm,
                              am_dealer_loc loc
                        WHERE ph.parent_group = pd.parent_group
                          AND ph.dealer_map_cd = pd.dealer_map_cd
                          AND ph.loc_cd = pd.loc_cd
                          AND ph.comp_fa = pd.comp_fa
                          AND ph.indent_num = pd.indent_num
                          AND ph.indent_date >= ld_frm_date
                          AND ph.indent_date < ld_to_date
                          AND pd.part_num = pm.part_num
                          AND pm.dealer_map_cd = loc.principal_map_cd
                          AND pm.principal_map_cd = loc.principal_map_cd
                          AND NVL(pd.indent_qty, 0) - NVL(pd.issued_qty, 0) > 0
                          AND NVL(ph.indent_status, 'H') = 'R'
                          AND nvl(pd.cancel_yn, 'N') <> 'Y'
                          AND loc.dealer_map_cd = ph.to_dealer
                          AND loc.loc_cd = ph.indent_to
                        GROUP BY ph.to_dealer, ph.indent_to, pm.part_num) loop
        BEGIN
          INSERT INTO PT_CBO_AOS
            (dealer_map_cd,
             loc_cd,
             part_num,
             cbo,
             pend_ord,
             created_date,
             cbo_type,
             cbo_date)
          VALUES
            (m_indent.dealer_map_cd,
             m_indent.loc_cd,
             m_indent.part_num,
             m_indent.cbo,
             m_indent.pend_ord,
             m_indent.created_date,
             m_indent.cbo_type,
             m_indent.cbo_date);
        EXCEPTION
          WHEN DUP_VAL_ON_INDEX THEN
            P_ERR_CD  := 1;
            P_ERR_MSG := P_ERR_MSG || 'Duplicate value for' ||
                         m_indent.dealer_map_cd || '-' || m_indent.loc_cd;
          WHEN OTHERS THEN
            P_ERR_CD  := 1;
            P_ERR_MSG := P_ERR_MSG || m_indent.dealer_map_cd || '-' ||
                         m_indent.loc_cd;
        END;
      END LOOP;
      COMMIT;
    END;
  
    BEGIN
      for m_cust in (SELECT /*+ INDEX (PH IDX_PH_SO_DATE) */
                      ph.dealer_map_cd dealer_map_cd,
                      ph.loc_cd loc_cd,
                      pm.part_num,
                      SUM(pd.kill_qty) cbo,
                      Count(ph.so_num) pend_ord,
                      SYSDATE created_date,
                      'C' cbo_type,
                      trunc(SYSDATE) cbo_date
                       FROM ph_so         ph,
                            pd_so         pd,
                            pm_part       pm,
                            am_dealer_loc loc
                      WHERE ph.so_date >= ld_frm_date
                        AND ph.so_date < ld_to_date
                           
                        AND ph.parent_group = pd.parent_group
                        AND ph.dealer_map_cd = pd.dealer_map_cd
                        AND ph.loc_cd = pd.loc_cd
                        AND ph.comp_fa = pd.comp_fa
                        AND ph.so_num = pd.so_num
                        AND pd.part_num = pm.part_num
                        AND pm.dealer_map_cd = loc.principal_map_cd
                        AND pm.principal_map_cd = loc.principal_map_cd
                        AND NVL(pd.kill_qty, 0) > 0
                        AND nvl(pd.cancel_yn, 'N') <> 'Y'
                        AND loc.dealer_map_cd = ph.dealer_map_cd
                        AND loc.loc_cd = ph.loc_cd                    
                      GROUP BY ph.dealer_map_cd, ph.loc_cd, pm.part_num) loop
        BEGIN
          INSERT INTO PT_CBO_AOS
            (dealer_map_cd,
             loc_cd,
             part_num,
             cbo,
             pend_ord,
             created_date,
             cbo_type,
             cbo_date)
          VALUES
            (m_cust.dealer_map_cd,
             m_cust.loc_cd,
             m_cust.part_num,
             m_cust.cbo,
             m_cust.pend_ord,
             m_cust.created_date,
             m_cust.cbo_type,
             m_cust.cbo_date);
        EXCEPTION
          WHEN DUP_VAL_ON_INDEX THEN
            P_ERR_CD  := 1;
            P_ERR_MSG := P_ERR_MSG || 'Duplicate value for' ||
                         m_cust.dealer_map_cd || '-' || m_cust.loc_cd;
          WHEN OTHERS THEN
            P_ERR_CD  := 1;
            P_ERR_MSG := P_ERR_MSG || m_cust.dealer_map_cd || '-' ||
                         m_cust.loc_cd;
        END;
      END LOOP;
      COMMIT;
    END;
  
    BEGIN
      for m_req in (SELECT /*+ index(vt IDX_VT_REQ_DATE) */
                     vt.dealer_map_cd dealer_map_cd,
                     vt.loc_cd loc_cd,
                     pm.Part_Num,
                     SUM(NVL(vt.req_qty, 0) - NVL(vt.iss_qty, 0)) cbo,
                     Count(vt.part_num) pend_ord,
                     SYSDATE created_date,
                     'R' cbo_type,
                     trunc(SYSDATE) cbo_date
                      FROM vt_req vt, pm_part pm, am_dealer_loc loc
                     WHERE vt.req_date >= ld_frm_date
                       AND vt.req_date < ld_to_date
                       AND NVL(vt.req_qty, 0) - NVL(vt.iss_qty, 0) > 0
                       AND NVL(vt.req_status, 'O') != 'C'
                       AND vt.part_num = pm.part_num
                       AND pm.dealer_map_cd = loc.principal_map_cd
                       AND pm.principal_map_cd = loc.principal_map_cd
                       AND loc.dealer_map_cd = vt.dealer_map_cd
                       AND loc.loc_cd = vt.loc_cd
                     GROUP BY vt.dealer_map_cd, vt.loc_cd, pm.part_num) loop
        BEGIN
          INSERT INTO PT_CBO_AOS
            (dealer_map_cd,
             loc_cd,
             part_num,
             cbo,
             pend_ord,
             created_date,
             cbo_type,
             cbo_date)
          VALUES
            (m_req.dealer_map_cd,
             m_req.loc_cd,
             m_req.part_num,
             m_req.cbo,
             m_req.pend_ord,
             m_req.created_date,
             m_req.cbo_type,
             m_req.cbo_date);
        
        EXCEPTION
          WHEN DUP_VAL_ON_INDEX THEN
            P_ERR_CD  := 1;
            P_ERR_MSG := P_ERR_MSG || 'Duplicate value for' ||
                         m_req.dealer_map_cd || '-' || m_req.loc_cd;
          WHEN OTHERS THEN
            P_ERR_CD  := 1;
            P_ERR_MSG := P_ERR_MSG || m_req.dealer_map_cd || '-' ||
                         m_req.loc_cd;
        END;
      END LOOP;
      COMMIT;
    END;
  
  EXCEPTION
    WHEN OTHERS THEN
      p_err_cd  := 1;
      p_err_msg := p_err_msg || SQLERRM;
    
  END;
  -------------------END-----------------------------------------
  PROCEDURE SP_REPLACEMENT_DATA(P_ERR_CD  OUT NUMBER,
                                P_ERR_MSG OUT VARCHAR2) IS
  BEGIN
    p_err_cd := 0;
  
    INSERT INTO REPLACE_PLANNING_TOOL
      (ROW_TRANS_TYPE,
       RELPACING_ITEM_CODE,
       RELPACING_PREFIX,
       RELPACED_ITEM_CODE,
       RELPACED_PREFIX,
       WAREHOUSE_CODE,
       WAREHOUSE_GROUP_CODE,
       EXTRACTION_DATE,
       CREATED_DATE,
       INHERIT_STOCK,
       REPLACEMENT_MULTIPLIER,
       REPLACEMENT_DESCRIPTION,
       FREE_TEXT_1,
       FREE_TEXT_2)
      select 'M', --Raw_Transaction_Type,
             T4.LATEST_PART, -- Replacing_Item_Code, --New Part
             NULL, --Replacing_Prefix,
             T1.PART_NUM, -- Replaced_Item_code, --Old Part
             NULL, -- Replaced_Prefix,
             t1.PARENT_GROUP || '-' || t1.Dealer_Map_Cd || '-' || t1.LOC_CD, --  Warehouse_code,
             (select am.region_cd
                from am_dealer_loc am
               where am.parent_group = t1.PARENT_GROUP
                 and am.dealer_map_cd = t1.DEALER_MAP_CD
                 and am.loc_cd = t1.LOC_CD) || '_' || t1.PARENT_GROUP, -- Warehouse_Grp_Cd,
             to_char(sysdate, 'YYYYMMDD') || 'T' ||
             to_char(sysdate, 'HH24MI'), -- Extraction_date,
             sysdate,
             'Y', -- Inherit_Stock,
             1, -- Replacement_Multiplier,
             'NULL', --  Replacement_Desc,
             T4.ITEM_ISSUE_IND, --  Free_text1,
             null --  Free_text2
        from (SELECT distinct PPS.PART_NUM,
                              PPS.WAREHOUSE_CODE,
                              regexp_substr(Pps.warehouse_code,
                                            '[^-]+',
                                            1,
                                            1) PARENT_GROUP,
                              regexp_substr(Pps.warehouse_code,
                                            '[^-]+',
                                            1,
                                            2) DEALER_MAP_CD,
                              regexp_substr(Pps.warehouse_code,
                                            '[^-]+',
                                            1,
                                            3) LOC_CD,
                              (SELECT al.list_code
                                 FROM AM_LIST al
                                WHERE al.principal_map_cd = 1
                                  and al.list_name = 'RPDC_SOURCE'
                                  and al.list_desc =
                                      (SELECT GET_PART_SOURCE(am.dealer_map_cd,
                                                              am.parent_group,
                                                              am.loc_cd,
                                                              PPS.PART_NUM)
                                         FROM DUAL)) List_Cd
                FROM pm_part_selection_poc PPS, AM_DEALER_LOC AM
               WHERE AM.PARENT_GROUP =
                     regexp_substr(Pps.warehouse_code, '[^-]+', 1, 1)
                 AND AM.DEALER_MAP_CD =
                     regexp_substr(Pps.warehouse_code, '[^-]+', 1, 2)
                 AND AM.LOC_CD =
                     regexp_substr(Pps.warehouse_code, '[^-]+', 1, 3)
                 AND Am.Principal_Map_Cd = 1 --added by pooja on 07-06-2023
              -- AND AM.CONSG_CD = '0211'
              ) t1,
             ITEM_DETL_LINK T4
       where t1.PART_NUM = t4.item_code
         and t1.List_Cd = t4.item_comp_code;
  
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      p_err_cd  := 1;
      p_err_msg := p_err_msg || SQLERRM;
    
  END SP_REPLACEMENT_DATA;

  PROCEDURE SP_SUPPLIER_DATA(P_ERR_CD OUT NUMBER, P_ERR_MSG OUT VARCHAR2) IS
  BEGIN
    p_err_cd := 0;
  
    INSERT INTO SUPPLIER_PLANNING_TOOL
      (SELECT distinct 'M' Row_transaction_type,
                       'ITEM' Supplier_Data_Type,
                       PPV.PART_NUM PART_NUMBER,
                       NULL PREFIX,
                       AM.PARENT_GROUP || '-' || AM.DEALER_MAP_CD || '-' ||
                       AM.LOC_CD WAREHOUSE_CODE,
                       AM.REGION_CD || '_' || AM.PARENT_GROUP WAREHOUSE_GRP_CD,
                       /*CASE
                         WHEN AM.SPARES_STOCK_YARD_YN = 'Y' THEN
                          GET_PART_SOURCE(AM.DEALER_MAP_CD,
                                          AM.PARENT_GROUP,
                                          AM.LOC_CD,
                                          PM.PART_NUM)
                         ELSE
                          (SELECT AM1.PARENT_GROUP || '-' ||
                                  AM1.DEALER_MAP_CD || '-' || AM1.LOC_CD
                             FROM AM_DEALER_LOC AM1
                            WHERE AM1.PARENT_GROUP = AM.PARENT_GROUP                                 
                              AND AM1.CONSG_CD = AM.CONSG_CD
                              AND AM1.SPARES_STOCK_YARD_YN = 'Y'
                              and rownum = 1)
                       END SUPPLIER_CODE,*/
                       
                         CASE
                WHEN PM.CATG_CD <> 'AA' then
                 CASE
                   WHEN AM.SPARES_STOCK_YARD_YN = 'Y' THEN
                    GET_PART_SOURCE(AM.DEALER_MAP_CD,
                                    AM.PARENT_GROUP,
                                    AM.LOC_CD,
                                    PM.PART_NUM)
                   ELSE
                    (SELECT AM1.PARENT_GROUP || '-' || AM1.DEALER_MAP_CD || '-' ||
                            AM1.LOC_CD
                       FROM AM_DEALER_LOC AM1
                      WHERE AM1.PARENT_GROUP = AM.PARENT_GROUP
                           --AND AM1.DEALER_MAP_CD = AM.DEALER_MAP_CD
                        AND AM1.CONSG_CD = AM.CONSG_CD
                        AND AM1.SPARES_STOCK_YARD_YN = 'Y'
                        and rownum = 1)
                 END
                 WHEN PM.CATG_CD = 'AA' Then
                CASE
                  WHEN AM.SPARES_STOCK_YARD_YN = 'Y' and
                       (am.consg_cd =
                       (SELECT ac.acc_ordering_consignee
                           from am_acc_dealer_code_map ac
                          where ac.loc_cd = am.loc_cd
                            and am.outlet_cd = ac.outlet_cd
                            and am.mul_dealer_cd = ac.dealer_cd
                            and am.principal_map_cd = 1) OR
                       ((SELECT acc_ordering_consignee
                            from am_acc_dealer_code_map ac
                           where ac.loc_cd = am.loc_cd
                             and am.outlet_cd = ac.outlet_cd
                             and am.mul_dealer_cd = ac.dealer_cd
                             and am.principal_map_cd = 1) IS NULL)) THEN
                   GET_PART_SOURCE(AM.DEALER_MAP_CD,
                                   AM.PARENT_GROUP,
                                   AM.LOC_CD,
                                   PM.PART_NUM) 
                  WHEN AM.SPARES_STOCK_YARD_YN = 'Y' THEN
                   (SELECT AM1.PARENT_GROUP || '-' || AM1.DEALER_MAP_CD || '-' ||
                           AM1.LOC_CD
                      FROM AM_DEALER_LOC AM1, AM_ACC_DEALER_CODE_MAP AC1
                     WHERE AM1.PARENT_GROUP = AM.PARENT_GROUP
                       AND AM1.CONSG_CD = AC1.Acc_ordering_consignee
                       AND AM1.SPARES_STOCK_YARD_YN = 'Y'
                       and am.loc_cd = ac1.loc_cd
                       and am.outlet_cd = ac1.outlet_cd
                       and am.mul_dealer_cd = ac1.dealer_cd
                       and am.principal_map_cd = 1
                       and rownum = 1)              
                  WHEN nvl(AM.SPARES_STOCK_YARD_YN, 'N') <> 'Y' and
                       am.loc_cd =
                       (SELECT ac.loc_cd
                          from am_acc_dealer_code_map ac
                         where ac.loc_cd = am.loc_cd
                           and am.outlet_cd = ac.outlet_cd
                           and am.mul_dealer_cd = ac.dealer_cd
                           and am.principal_map_cd = 1
                           and ac.acc_ordering_consignee is not null)
                        THEN
                   (SELECT AM1.PARENT_GROUP || '-' || AM1.DEALER_MAP_CD || '-' ||
                           AM1.LOC_CD
                      FROM AM_DEALER_LOC AM1, AM_ACC_DEALER_CODE_MAP AC1
                     WHERE AM1.PARENT_GROUP = AM.PARENT_GROUP
                       AND AM1.CONSG_CD = AC1.Acc_ordering_consignee
                       AND AM1.SPARES_STOCK_YARD_YN = 'Y'
                       and am.loc_cd = ac1.loc_cd
                       and am.outlet_cd = ac1.outlet_cd
                       and am.mul_dealer_cd = ac1.dealer_cd
                       and am.principal_map_cd = 1
                       and rownum = 1)
                  ELSE
                   (SELECT AM1.PARENT_GROUP || '-' || AM1.DEALER_MAP_CD || '-' ||
                           AM1.LOC_CD
                      FROM AM_DEALER_LOC AM1
                     WHERE AM1.PARENT_GROUP = AM.PARENT_GROUP                         
                       AND AM1.CONSG_CD = AM.CONSG_CD
                       AND AM1.SPARES_STOCK_YARD_YN = 'Y'
                       and rownum = 1)
                 END
                END SUPPLIER_CODE,  --supplier code changes 14-APR-2023
                       '' MARKET_CODE,
                       PPV.PART_NUM MASTER_ITEM_CODE,
                       CASE
                         WHEN AM.SPARES_STOCK_YARD_YN = 'Y' THEN
                          GET_PART_SOURCE(AM.DEALER_MAP_CD,
                                          AM.PARENT_GROUP,
                                          AM.LOC_CD,
                                          PM.PART_NUM)
                         ELSE
                          (SELECT AM1.PARENT_GROUP || '-' ||
                                  AM1.DEALER_MAP_CD || '-' || AM1.LOC_CD
                             FROM AM_DEALER_LOC AM1
                            WHERE AM1.PARENT_GROUP = AM.PARENT_GROUP                                 
                              AND AM1.CONSG_CD = AM.CONSG_CD
                              AND AM1.SPARES_STOCK_YARD_YN = 'Y'
                              and rownum = 1)
                       END Master_Supplier_Code,
                       '' Hazardous,
                       '' Returnable,
                       '' Replacement,
                       '' Replacement_desc,
                       '' DELIVERY_MODE_CD,
                       to_char(sysdate, 'YYYYMMDD') || 'T' ||
                       to_char(sysdate, 'HH24MI') EXTRACTION_DATE,
                       case
                         when CASE
                                WHEN AM.SPARES_STOCK_YARD_YN = 'Y' THEN
                                 GET_PART_SOURCE(AM.DEALER_MAP_CD,
                                                 AM.PARENT_GROUP,
                                                 AM.LOC_CD,
                                                 PM.PART_NUM)
                                ELSE
                                 (SELECT AM1.PARENT_GROUP || '-' ||
                                         AM1.DEALER_MAP_CD || '-' || AM1.LOC_CD
                                    FROM AM_DEALER_LOC AM1
                                   WHERE AM1.PARENT_GROUP = AM.PARENT_GROUP                                      
                                     AND AM1.CONSG_CD = AM.CONSG_CD
                                     AND AM1.SPARES_STOCK_YARD_YN = 'Y'                                  
                                  )
                              END = 'GGN' then
                          ceil(decode(nvl(AP.Mul_Lead_Time, 1),
                                      0,
                                      1,
                                      nvl(AP.Mul_Lead_Time, 1)))
                         else
                          ceil(decode(AP.RPDC_LEAD_TIME,
                                      null,
                                      decode(nvl(AP.Mul_Lead_Time, 1),
                                             0,
                                             1,
                                             nvl(AP.Mul_Lead_Time, 1)),
                                      decode(nvl(AP.RPDC_LEAD_TIME, 1),
                                             0,
                                             1,
                                             nvl(AP.RPDC_LEAD_TIME, 1))))
                       end as Lead_time,
                       ceil(PM.MIN_ORD_QTY) Minimum_order_qty,
                       99999 Maximum_order_qty,
                       ceil(PM.MIN_ORD_QTY) Multiple_order_qty,
                       '' Purchase_price,
                       '' Purchase_price_currency,
                       '' Duty_Cost,
                       '' Duty_Cost_Curr,
                       '' Other_Cost,
                       '' Other_Cost_Curr,
                       '' Transport_Cost,
                       '' Transport_Cost_Curr,
                       '' Free_Text1,
                       '' Free_Text2,
                       '' Bulk_Order_qty1,
                       '' Bulk_Order_qty2,
                       '' Discount_order_qty_limit1,
                       '' Discount_ordre_qty_limit2,
                       '' Discount_item_price1,
                       '' Discount_item_price_curr1,
                       '' Discount_item_price2,
                       '' Discount_item_price_curr2,
                       '' Use_Rev_trans_order,
                       '' Use_in_plan_event_order,
                       '' Supplier_email_address,
                       '' Supplier_item_code,
                       SYSDATE Created_date
         FROM PM_PART_VAR           PPV,
              PM_PART               PM,
              AM_DEALER_LOC         AM,
              AM_DEALER_LOC_PARAM   AP,
              pm_part_selection_poc PPS
        WHERE PPV.PART_NUM = PM.PART_NUM
          AND PM.DEALER_MAP_CD = 1
          AND PPV.DEALER_MAP_CD = AM.DEALER_MAP_CD
          AND PPV.LOC_CD = AM.LOC_CD
          AND PPV.PARENT_GROUP = AM.PARENT_GROUP
          AND AM.PRINCIPAL_MAP_CD = 1
          AND AM.Parent_Group = AP.Parent_Group
          AND AM.DEALER_MAP_CD = AP.DEALER_MAP_CD
          AND AM.LOC_CD = AP.LOC_CD
          AND AM.Parent_Group =
              regexp_substr(Pps.warehouse_code, '[^-]+', 1, 1)
          AND AM.DEALER_MAP_CD =
              regexp_substr(Pps.warehouse_code, '[^-]+', 1, 2)
          AND AM.LOC_CD = regexp_substr(Pps.warehouse_code, '[^-]+', 1, 3)
          AND PM.PART_NUM = PPS.Part_Num
          AND PM.CATG_CD NOT IN ('A', 'L', 'U', 'UA', 'OIL', 'T'));
  
  EXCEPTION
    WHEN OTHERS THEN
      p_err_cd  := 1;
      p_err_msg := p_err_msg || SQLERRM;
  END SP_SUPPLIER_DATA;

end pkg_planning_tool_pan_india;
/
