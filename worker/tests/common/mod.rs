// 测试公共辅助函数

use sqlx::postgres::{PgPool, PgPoolOptions};

use worker::event_store::PgEventStore;

/// 从环境变量或默认值获取测试数据库 URL
pub fn test_db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://worker:worker@localhost:5432/worker_test".into())
}

/// 创建测试数据库连接池
pub async fn create_test_pool() -> PgPool {
    let db_url = test_db_url();
    PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("无法连接测试数据库")
}

/// 创建测试 PgEventStore（每次创建新池，但迁移使用 IF NOT EXISTS 是幂等的）
pub async fn create_test_store() -> PgEventStore {
    let pool = create_test_pool().await;
    PgEventStore::run_migrations(&pool)
        .await
        .expect("数据库迁移失败");
    PgEventStore::from_pool(pool)
}

/// 清理测试数据（删除所有事件和快照）
pub async fn clean_test_data(pool: &PgPool) {
    let _ = sqlx::query("DELETE FROM snapshots").execute(pool).await;
    let _ = sqlx::query("DELETE FROM events").execute(pool).await;
}
