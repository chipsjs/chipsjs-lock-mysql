/**
 * 保证了原子性,lock_type 为0是互斥锁，1是自旋锁;使用前需要新创建一张table_name为METHOD_LOCK的表并且新增对应的methon_name
 *
 * 表的sql语句为:
 * CREATE TABLE `METHOD_LOCK` (
 `method_name` varchar(64) NOT NULL COMMENT '锁定的方法名',
 `expire_time` datetime DEFAULT NULL COMMENT 'lock过期时间',
 `status` tinyint(4) NOT NULL DEFAULT '0' COMMENT '0为free,1为lock',
 `desc` varchar(255) DEFAULT NULL COMMENT '备注信息',
 `lock_type` tinyint(4) NOT NULL DEFAULT '0' COMMENT '锁的类型，0为互斥锁，1为自旋锁',
 PRIMARY KEY (`method_name`)
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 *
 * @author chips
 * @date 2019/04/21
 */

const Mysql = require("chipsjs-mysql");
const Utils = require("chipsjs-utils");
const Timer = require("chipsjs-timer");

let time = {
    one_millisecond: 1,
    one_second: 1000,
    one_minute: 1000*60,
    one_hour: 1000*60*60,
    one_day: 24*60*60*1000
};

let Lock = {};
//这个锁的过期时间是两分钟;
Lock.getMutexLock = async(method_name, server_id) => {
    let sql = "update METHOD_LOCK set owner_id = " + Mysql.escape(server_id) + ", expire_time = '" + Utils.TimeUtils.outOtherTimeStamp(true, time.one_minute * 2) + "' where lock_type = 0 and method_name = '" + method_name + "' and (owner_id = " + Mysql.escape(server_id) + " or owner_id = NULL or expire_time < '" + TimeUtils.outTimestamp() + "');";

    let update_rows = await Mysql.awaitCommonWithoutParam(sql);

    return update_rows.affectedRows !== 0;
};

//to optimize,释放锁根据ownerid释放!!
Lock.releaseMutexLock = async(method_name) => {
    let sql = "update METHOD_LOCK set status = 0, expire_time = '" + Utils.TimeUtils.outTimestamp() + "' where lock_type = 0 and method_name = '" + method_name + "';";

    let update_rows = await Mysql.awaitCommonWithoutParam(sql);

    return update_rows.affectedRows !== 0;
};

//自旋锁的时间设置为10s，为了防止进程挂掉后影响其他进程的正常服务;
Lock.getSpinLock = async(method_name) => {
    let sql = "update METHOD_LOCK set status = 1, expire_time = '" + TimeUtils.outOtherTimeStamp(true, time.one_second * 10) + "' where lock_type = 1 and method_name = '" + method_name + "' and (status = 0 or expire_time < '" + TimeUtils.outTimestamp() + "');";

    let update_rows = await Mysql.awaitCommonWithoutParam(sql);
    let wait_get_lock = (update_rows.affectedRows === 0);

    while(wait_get_lock)
    {
        await Timer.sleep(time.one_millisecond * 50);

        update_rows = await Mysql.awaitCommonWithoutParam(sql);
        wait_get_lock = (update_rows.affectedRows === 0);
    }

    return !wait_get_lock;
};

Lock.releaseSpinLock = async(method_name) => {
    let sql = "update METHOD_LOCK set status = 0, expire_time = '" + Utils.TimeUtils.outTimestamp() + "' where lock_type = 1 and method_name = '" + method_name + "';";

    let update_rows = await Mysql.awaitCommonWithoutParam(sql);

    return update_rows.affectedRows !== 0;
};

module.exports = Lock;