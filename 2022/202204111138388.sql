INSERT INTO t_touch_preorder_action_rule
(filter_el, `action`, action_type, action_name, state, sort, province, limit_tag, app_id, psptid_limit, bookingnum_limit)
VALUES ('#isNotEmpty(#root[GRAB_STAFF_ID])', 'preOrderComingSoon', '2', '即将上门', '1', 0, 'zzzz',  NULL, NULL, NULL, '1');


INSERT INTO `t_touch_interface_config`
(`app_env`,`module`,`intf_type`,`intf_name`,`intf_desc`,`intf_service`,`req_method`,`input_xml`,`output_xml`,`last_update`,`socketTimeout`,`connectTimeout`)
VALUES (null,'app-server','ability','smartHomeEngineer','查询是否是智家工程师','http://10.245.50.26:8000/api/chinaUnicom/channelOperation/digitalServices/smartHomeEngineer/v1','post','smartHomeEngineer/smartHomeEngineerInput','smartHomeEngineer/smartHomeEngineerOutput',null,null,null);