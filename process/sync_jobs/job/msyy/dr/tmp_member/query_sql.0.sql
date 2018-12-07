select 
	tc.VIPCARDNO member_code, 
	tc.CARDFACENO, 
	tvi.VIPNAME member_name, 
	tvi.BIRTHDAY baby_birthday, 
	case when length(tvi.mobile)=11 then tvi.mobile when length(tvi.TELPHONE)=11 then tvi.TELPHONE end AS mobile, 
	tc.REGDATE create_date, 
	tvi.ORGCODE reg_shop_code, 
	case tc.VIPSTATUS when '1' then '0' else '1' end member_state, 
	tct.CARDTYPECODE member_type, 
	tct.CARDTYPENAME member_type_name, 
	case when tvi.CHILDCONSULTANT is not null then tvi.CHILDCONSULTANT else tvi.FkrCode end guider_code
from TISUCARD tc
inner join TISUVIPINFO tvi 
on tc.VIPINFONO=tvi.VIPINFONO
inner join TBASCARDTYPE tct 
on tc.CARDTYPECODE=tct.CARDTYPECODE
left join TINTCARDUPDINFO tcui 
on tc.VIPCARDNO=tcui.VIPCARDNO
where to_date(tc.REGDATE,'yy-mm-dd hh24:mi:ss')>=trunc(sysdate -7)
or to_date(tcui.UPDATEDATE,'yy-mm-dd hh24:mi:ss')>=trunc(sysdate -7)
