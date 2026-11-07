package com.flyingzc.azir.tso.mysql;

import javax.sql.DataSource;

/** 标识与 MySQL 控制面共享同一数据源的时间戳权威。 */
public interface MysqlTimestampAuthority {
    /** 只有确定使用同一数据源对象时才允许走服务器端合并发布。 */
    boolean usesDataSource(DataSource dataSource);
}
