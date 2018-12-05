select 
	tc.VIPCARDNO member_code, 
	tc.VIPCARDNO,
	tvi.VIPNAME member_name, 
	tvi.BIRTHDAY baby_birthday, 
	case when length(tvi.mobile)=11 then tvi.mobile when length(tvi.TELPHONE)=11 then tvi.TELPHONE end AS mobile, 
	tc.REGDATE create_date, 
	tvi.ORGCODE reg_shop_code
from TISUCARD tc
inner join TISUVIPINFO tvi 
on tc.VIPINFONO=tvi.VIPINFONO
left join TINTCARDUPDINFO tcui
on tc.VIPCARDNO=tcui.VIPCARDNO
where to_date(tc.REGDATE,'yy-mm-dd hh24:mi:ss')>=trunc(sysdate)
or to_date(tcui.UPDATEDATE,'yy-mm-dd hh24:mi:ss')>=trunc(sysdate)