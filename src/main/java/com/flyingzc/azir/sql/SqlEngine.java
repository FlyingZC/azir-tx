package com.flyingzc.azir.sql;

/** 事务 SQL 会话的公开工厂接口，后端在应用启动时显式选定。 */
public interface SqlEngine {
    /** 每个调用方取得独立会话，提交合同由具体后端实现声明。 */
    SqlSession openSession();
}
