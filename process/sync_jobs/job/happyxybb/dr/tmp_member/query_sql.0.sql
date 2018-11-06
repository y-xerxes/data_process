SELECT
a.id AS member_code,
a.no as card_no,
a.name AS member_name,
a.birthday  AS baby_birthday,
a.mobile AS mobile,
a.create_date AS create_date,
a.office_id AS reg_shop_code,
a.status AS member_state,
a.level_id member_type,
b.name member_type_name
FROM
ms_member_def AS a
inner join ms_member_level b on a.level_id=b.id
WHERE a.update_date >= date_sub(curdate(),interval 7 day)