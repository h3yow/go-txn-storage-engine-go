package storage

// 本文件提供仅用于测试的内部钩子（包内测试可直接设置字段，
// 外部测试包通过下面的 helper 注入崩溃点）。

// setCrashHook 安装/卸载回收崩溃注入钩子。仅供测试使用。
func (db *DB) setCrashHook(fn func(phase string)) { db.crashHook = fn }
