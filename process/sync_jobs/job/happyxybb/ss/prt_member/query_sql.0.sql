SELECT
a.id  member_code,
a.no  member_no,
a.name  AS member_name,
a.birthday    AS baby_birthdate,
a.mobile member_mobile,
a.create_date  AS rgst_date,
a.office_id  AS rgst_shop_code
FROM
ms_member_def  AS a
WHERE
a.create_date >= curdate()