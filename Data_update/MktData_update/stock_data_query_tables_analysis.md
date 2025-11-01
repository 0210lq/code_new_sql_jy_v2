# 股票数据查询使用的表和字段分析

## 📊 使用的数据库表

### 查询1：普通市场数据（沪深主板和创业板）

| 表名 | 别名 | 用途 | 实际使用的字段 | 备注 |
|------|------|------|----------------|------|
| `jydb.SecuMain` | t2 | 股票基本信息表 | `SecuCode`, `SecuMarket`, `CompanyCode`, `InnerCode`, `SecuCategory`, `ListedSector`, `ListedState` | ✅ 必需 |
| `jydb.LC_MainIndexNew` | t1 | 财务指标表（子查询） | `EPSTTM`, `NetAssetPS`, `OperCashFlowPSTTM`, `OperatingRevenuePSTTM`, `EndDate`, `CompanyCode` | ✅ 必需 |
| `jydb.LC_MainDataNew` | - | 财务数据日期确认表（子查询） | `EndDate`, `CompanyCode`, `Mark`, `InfoPublDate` | ⚠️ **仅用于WHERE子查询，不返回数据** |
| `jydb.QT_stockPerformance` | t3 | 股票行情表 | `ClosePrice`, `ChangePCT`, `TurnoverRate`, `TotalMV`, `TradingDay`, `InnerCode` | ✅ 必需 |

### 查询2：科创板财务数据

| 表名 | 别名 | 用途 | 实际使用的字段 | 备注 |
|------|------|------|----------------|------|
| `jydb.SecuMain` | t2 | 股票基本信息表 | `SecuCode`, `SecuMarket`, `CompanyCode`, `InnerCode`, `SecuCategory`, `ListedSector`, `ListedState` | ✅ 必需 |
| `jydb.LC_STIBMainIndex` | t1 | 科创板财务指标表（子查询） | `EPSTTM`, `NetAssetPS`, `OperCashFlowPSTTM`, `OperatingRevenuePSTTM`, `EndDate`, `CompanyCode`, `IfAdjusted`, `InfoPublDate` | ✅ 必需 |
| `jydb.LC_STIBDailyQuote` | t3 | 科创板行情表（子查询） | `ClosePrice`, `TradingDay`, `InnerCode` | ✅ 必需 |

### 查询3：科创板行情数据

| 表名 | 别名 | 用途 | 实际使用的字段 | 备注 |
|------|------|------|----------------|------|
| `jydb.LC_STIBPerformance` | t1 | 科创板行情表 | `ClosePrice`, `ChangePCT`, `TurnoverRate`, `TotalMV`, `TradingDay`, `InnerCode` | ✅ 必需 |
| `jydb.SecuMain` | t2 | 股票基本信息表 | `SecuCode`, `SecuMarket`, `SecuCategory`, `ListedSector`, `InnerCode` | ✅ 必需 |
| `jydb.LC_STIBAdjustingFactor` | t3 | 科创板复权因子表 | **无字段被使用** | ❌ **完全不需要，可以删除** |

---

## ❌ 不需要/冗余的数据

### 1. **`jydb.LC_STIBAdjustingFactor` 表** 
**位置：** 查询3（科创板行情数据）中的 LEFT JOIN

```sql
LEFT JOIN jydb.LC_STIBAdjustingFactor t3 
    ON t1.InnerCode = t3.InnerCode AND t1.TradingDay = t3.ExDiviDate
```

**问题：**
- ❌ 这个表被LEFT JOIN但**从未在SELECT、WHERE或任何地方使用**
- ❌ 在WHERE子句中也没有对t3的任何条件判断
- ❌ 原始MATLAB代码中有这个JOIN，但可能只是为了预留功能

**建议：** 可以删除这个JOIN，不会影响查询结果

### 2. **`jydb.LC_MainDataNew` 表的数据返回**
**位置：** 查询1中的子查询

```sql
SELECT MAX(EndDate)
FROM jydb.LC_MainDataNew
WHERE Mark = 2 AND CompanyCode = t1.CompanyCode AND InfoPublDate <= %s
```

**说明：**
- ⚠️ 这个表**只用于子查询来确定最新的EndDate**
- ⚠️ **不返回任何实际数据给最终结果集**
- ✅ 这是必要的，用于过滤`LC_MainIndexNew`表中最新的财务数据

**结论：** 这是必要的逻辑，不是冗余

---

## 📋 最终返回的字段

| 字段名 | 来源 | 说明 |
|--------|------|------|
| `S_INFO_WINDCODE` | 从`SecuCode`转换而来（添加.SH/.SZ后缀） | 股票代码 |
| `TRADE_DT` | `TradingDay`格式化（YYYYMMDD） | 交易日期 |
| `ClosePrice` | `QT_stockPerformance`或`LC_STIBPerformance` | 收盘价 |
| `ChangePCT` | `QT_stockPerformance`或`LC_STIBPerformance` | 涨跌幅 |
| `TurnoverRate` | `QT_stockPerformance`或`LC_STIBPerformance` | 换手率 |
| `TotalMV` | `QT_stockPerformance`或`LC_STIBPerformance` | 总市值 |
| `PE_TTM` | `ClosePrice / EPSTTM`（计算字段） | 市盈率TTM |
| `PB` | `ClosePrice / NetAssetPS`（计算字段） | 市净率 |
| `PCF_OCFTTM` | `ClosePrice / OperCashFlowPSTTM`（计算字段） | 市现率TTM |
| `PS_TTM` | `ClosePrice / OperatingRevenuePSTTM`（计算字段） | 市销率TTM |

---

## 🔧 优化建议

### 建议1：删除冗余的 JOIN

在查询3中，可以移除`LC_STIBAdjustingFactor`表的JOIN：

```sql
-- 当前代码（第201-202行）
LEFT JOIN jydb.LC_STIBAdjustingFactor t3 
    ON t1.InnerCode = t3.InnerCode AND t1.TradingDay = t3.ExDiviDate

-- 建议删除，因为t3没有被使用
```

### 建议2：优化查询3的 SELECT（如果确实不需要复权因子）

如果需要复权因子数据，应该在SELECT中包含相关字段；如果不需要，应删除JOIN。

---

## 📝 总结

**使用的表（共7个）：**
1. ✅ `jydb.SecuMain` - 必需
2. ✅ `jydb.LC_MainIndexNew` - 必需
3. ✅ `jydb.LC_MainDataNew` - 必需（用于子查询过滤）
4. ✅ `jydb.QT_stockPerformance` - 必需
5. ✅ `jydb.LC_STIBMainIndex` - 必需
6. ✅ `jydb.LC_STIBDailyQuote` - 必需
7. ✅ `jydb.LC_STIBPerformance` - 必需
8. ❌ `jydb.LC_STIBAdjustingFactor` - **不需要，建议删除**

