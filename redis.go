// Copyright (c) 2024 The horm-database Authors (such as CaoHao <18500482693@163.com>). All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package orm

// Expire 设置 key 的过期时间，key 过期后将不再可用。单位以秒计。
// param: key string
// param: int ttl 到期时间，ttl秒
func (o *ORM) Expire(key string, ttl int) *ORM {
	o.query.Expire(key, ttl)
	return o
}

// TTL 以秒为单位返回 key 的剩余过期时间。
// param: string key
func (o *ORM) TTL(key string) *ORM {
	o.query.TTL(key)
	return o
}

// Exists 查看值是否存在 exists
// param: key string
func (o *ORM) Exists(key string) *ORM {
	o.query.Exists(key)
	return o
}

// Del 删除已存在的键。不存在的 key 会被忽略。
// param: key string
func (o *ORM) Del(key string) *ORM {
	o.query.Del(key)
	return o
}

// Set 设置给定 key 的值。如果 key 已经存储其他值， Set 就覆写旧值。
// param: key string
// param: value interface{} 任意类型数据
// param: args ...interface{} set的其他参数
func (o *ORM) Set(key string, value interface{}, args ...interface{}) *ORM {
	o.query.Set(key, value, args...)
	return o
}

// SetEX 指定的 key 设置值及其过期时间。如果 key 已经存在， SETEX 命令将会替换旧的值。
// param: key string
// param: v interface{} 任意类型数据
// param: ttl int 到期时间
func (o *ORM) SetEX(key string, v interface{}, ttl int) *ORM {
	o.query.SetEX(key, v, ttl)
	return o
}

// SetNX redis.SetNX
// 指定的 key 不存在时，为 key 设置指定的值。
// param: key string
// param: v interface{} 任意类型数据
func (o *ORM) SetNX(key string, v interface{}) *ORM {
	o.query.SetNX(key, v)
	return o
}

// Get 获取指定 key 的值。如果 key 不存在，返回 nil 。可用 IsNil(err) 判断是否key不存在，如果key储存的值不是字符串类型，返回一个错误。
// param: key string
func (o *ORM) Get(key string) *ORM {
	o.query.Get(key)
	return o
}

// GetSet 设置给定 key 的值。如果 key 已经存储其他值， GetSet 就覆写旧值，并返回原来的值，如果原来未设置值，则返回报错 nil returned
// param: key string
// param: v interface{} 任意类型数据
func (o *ORM) GetSet(key string, v interface{}) *ORM {
	o.query.GetSet(key, v)
	return o
}

// Incr 将 key 中储存的数字值增一。 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCR 操作。 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
// param: key string
func (o *ORM) Incr(key string) *ORM {
	o.query.Incr(key)
	return o
}

// Decr 将 key 中储存的数字值减一。如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECR 操作。如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
// param: key string
func (o *ORM) Decr(key string) *ORM {
	o.query.Decr(key)
	return o
}

// IncrBy 将 key 中储存的数字加上指定的增量值。如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCRBY 命令。如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
// param: key string
// param: n string 自增数量
func (o *ORM) IncrBy(key string, n int) *ORM {
	o.query.IncrBy(key, n)
	return o
}

// MSet 批量设置一个或多个 key-value 对
// param: values map[string]interface{} // value will marshal
// 注意，本接口 Prefix 一定要在 MSet 之前设置，这里所有的 key 都会被加上 Prefix
func (o *ORM) MSet(values map[string]interface{}) *ORM {
	o.query.MSet(values)
	return o
}

// MGet 返回多个 key 的 value
// param: keys string
func (o *ORM) MGet(keys ...string) *ORM {
	o.query.MGet(keys...)
	return o
}

// SetBit 设置或清除指定偏移量上的位
// param: key string
// param: offset uint32 参数必须大于或等于 0 ，小于 2^32 (bit 映射被限制在 512 MB 之内)
// param: value bool true:设置为1,false：设置为0
func (o *ORM) SetBit(key string, offset uint32, value bool) *ORM {
	o.query.SetBit(key, offset, value)
	return o
}

// GetBit 获取指定偏移量上的位
// param: key string
// param: offset uint32 参数必须大于或等于 0 ，小于 2^32 (bit 映射被限制在 512 MB 之内)
func (o *ORM) GetBit(key string, offset uint32) *ORM {
	o.query.GetBit(key, offset)
	return o
}

// BitCount 计算给定字符串中，被设置为 1 的比特位的数量
// param: key string
// param: start int 可以使用负数值： 比如 -1 表示最后一个字节， -2 表示倒数第二个字节，以此类推
// param: end int 可以使用负数值： 比如 -1 表示最后一个字节， -2 表示倒数第二个字节，以此类推
func (o *ORM) BitCount(key string, start, end int) *ORM {
	o.query.BitCount(key, start, end)
	return o
}

// HSet 为哈希表中的字段赋值 。
// param: key string
// param: field interface{} 其中field建议为字符串,可以为整数，浮点数
// param: v interface{} 任意类型数据
// param: args ...interface{} 多条数据，按照filed,value 的格式，其中field建议为字符串,可以为整数，浮点数
func (o *ORM) HSet(key string, field, v interface{}, args ...interface{}) *ORM {
	o.query.HSet(key, field, v, args...)
	return o
}

// HSetNx 为哈希表中不存在的的字段赋值 。
// param: key string
// param: field string
// param: value interface{}
func (o *ORM) HSetNx(key string, filed interface{}, value interface{}) *ORM {
	o.query.HSetNx(key, filed, value)
	return o
}

// HmSet 把map数据设置到哈希表中。
// param: key string
// param: v map[string]interface{} 、或者 struct
func (o *ORM) HmSet(key string, v interface{}) *ORM {
	o.query.HmSet(key, v)
	return o
}

// HmGet 返回哈希表中，一个或多个给定字段的值。
// param: key string
// param: fields string 需要返回的域
func (o *ORM) HmGet(key string, fields ...string) *ORM {
	o.query.HmGet(key, fields...)
	return o
}

// HGet 数据从redis hget 出来之后反序列化并赋值给 v
// param: key string
// param: field string
func (o *ORM) HGet(key string, field interface{}) *ORM {
	o.query.HGet(key, field)
	return o
}

// HGetAll 返回哈希表中，所有的字段和值。
// param: key string
func (o *ORM) HGetAll(key string) *ORM {
	o.query.HGetAll(key)
	return o
}

// Hkeys 获取哈希表中的所有域（field）。
// param: key string
func (o *ORM) Hkeys(key string) *ORM {
	o.query.Hkeys(key)
	return o
}

// HIncrBy 为哈希表中的字段值加上指定增量值。
// param: key string
// param: field string
// param: n string 自增数量
func (o *ORM) HIncrBy(key string, field string, v int) *ORM {
	o.query.HIncrBy(key, field, v)
	return o
}

// HDel 删除哈希表 key 中的一个或多个指定字段，不存在的字段将被忽略。
// param: keyfield interface{}，删除指定key的field数据，这里输入的第一参数为key，其他为多个field，至少得有一个field
func (o *ORM) HDel(key string, field ...interface{}) *ORM {
	o.query.HDel(key, field...)
	return o
}

// HExists 查看哈希表的指定字段是否存在。
// param: key string
// param: field interface{}
func (o *ORM) HExists(key string, field interface{}) *ORM {
	o.query.HExists(key, field)
	return o
}

// HLen 获取哈希表中字段的数量。
// param: key string
func (o *ORM) HLen(key string) *ORM {
	o.query.HLen(key)
	return o
}

// HStrLen 获取哈希表某个字段长度。
// param: key string
// param: field string
func (o *ORM) HStrLen(key string, field interface{}) *ORM {
	o.query.HStrLen(key, field)
	return o
}

// HIncrByFloat 为哈希表中的字段值加上指定增量浮点数。
// param: key string
// param: field string
// param: v float64 自增数量
func (o *ORM) HIncrByFloat(key string, field string, v float64) *ORM {
	o.query.HIncrByFloat(key, field, v)
	return o
}

// HVals 返回所有的 value
// param: key string
func (o *ORM) HVals(key string) *ORM {
	o.query.HVals(key)
	return o
}

// LPush 将一个或多个值插入到列表头部。 如果 key 不存在，一个空列表会被创建并执行 LPUSH 操作。 当 key 存在但不是列表类型时，返回一个错误。
// param: key string
// param: v interface{} 任意类型数据
func (o *ORM) LPush(key string, v ...interface{}) *ORM {
	o.query.LPush(key, v...)
	return o
}

// RPush 将一个或多个值插入到列表的尾部(最右边)。如果列表不存在，一个空列表会被创建并执行 RPUSH 操作。 当列表存在但不是列表类型时，返回一个错误。
// param: key string
// param: v interface{} 任意类型数据
func (o *ORM) RPush(key string, v ...interface{}) *ORM {
	o.query.RPush(key, v...)
	return o
}

// LPop 移除并返回列表的第一个元素。
// param: key string
func (o *ORM) LPop(key string) *ORM {
	o.query.LPop(key)
	return o
}

// RPop 移除列表的最后一个元素，返回值为移除的元素。
// param: key string
func (o *ORM) RPop(key string) *ORM {
	o.query.RPop(key)
	return o
}

// LLen 返回列表的长度。 如果列表 key 不存在，则 key 被解释为一个空列表，返回 0 。 如果 key 不是列表类型，返回一个错误。
// param: key string
func (o *ORM) LLen(key string) *ORM {
	o.query.LLen(key)
	return o
}

// SAdd 将一个或多个成员元素加入到集合中，已经存在于集合的成员元素将被忽略。
// param: key string
// param: v ...interface{} 任意类型的多条数据，但是务必确保各条数据的类型保持一致
func (o *ORM) SAdd(key string, v ...interface{}) *ORM {
	o.query.SAdd(key, v...)
	return o
}

// SMembers 返回集合中的所有的成员。 不存在的集合 key 被视为空集合。
// param: key string
func (o *ORM) SMembers(key string) *ORM {
	o.query.SMembers(key)
	return o
}

// SRem 移除集合中的一个或多个成员元素，不存在的成员元素会被忽略
// param: key string
// param: v ...interface{} 任意类型的多条数据
func (o *ORM) SRem(key string, members ...interface{}) *ORM {
	o.query.SRem(key, members...)
	return o
}

// SCard 返回集合中元素的数量。
// param: key string
func (o *ORM) SCard(key string) *ORM {
	o.query.SCard(key)
	return o
}

// SIsMember 判断成员元素是否是集合的成员。
// param: key string
// param: member interface{} 要检索的任意类型数据
func (o *ORM) SIsMember(key string, member interface{}) *ORM {
	o.query.SIsMember(key, member)
	return o
}

// SRandMember 返回集合中的count个随机元素。
// param: key string
// param: count int 随机返回元素个数。
// 如果 count 为正数，且小于集合基数，那么命令返回一个包含 count 个元素的数组，数组中的元素各不相同。
// 如果 count 大于等于集合基数，那么返回整个集合。
// 如果 count 为负数，那么命令返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值。
func (o *ORM) SRandMember(key string, count int) *ORM {
	o.query.SRandMember(key, count)
	return o
}

// SPop 移除集合中的指定 key 的一个或多个随机成员，移除后会返回移除的成员。
// param: key string
// param: int count
func (o *ORM) SPop(key string, count int) *ORM {
	o.query.SPop(key, count)
	return o
}

// SMove 将指定成员 member 元素从 source 集合移动到 destination 集合。
// param: source string
// param: destination string
// param: member interface{} 要移动的成员，任意类型
func (o *ORM) SMove(source, destination string, member interface{}) *ORM {
	o.query.SMove(source, destination, member)
	return o
}

// ZAdd redis.ZAdd
// 将成员元素及其分数值加入到有序集当中。如果某个成员已经是有序集的成员，那么更新这个成员的分数值，并通过重新插入这个成员元素，
// 来保证该成员在正确的位置上。分数值可以是整数值或双精度浮点数。
// param: key string
// param: args ...interface{} 添加更多成员，需要按照  member, score, member, score 依次排列
// 注意：⚠️ 与 redis 命令不一样，需要按照  member, score, member, score, 格式传入
func (o *ORM) ZAdd(key string, args ...interface{}) *ORM {
	o.query.ZAdd(key, args...)
	return o
}

// ZRem 移除有序集中的一个或多个成员，不存在的成员将被忽略。
// param: key string
// param: members ...interface{} 任意类型的多条数据
func (o *ORM) ZRem(key string, members ...interface{}) *ORM {
	o.query.ZRem(key, members...)
	return o
}

// ZRemRangeByScore 移除有序集中，指定分数（score）区间内的所有成员。
// param: key string
// param: interface{} min max 分数区间，类型为整数或者浮点数
func (o *ORM) ZRemRangeByScore(key string, min, max interface{}) *ORM {
	o.query.ZRemRangeByScore(key, min, max)
	return o
}

// ZRemRangeByRank 移除有序集中，指定排名(rank)区间内的所有成员。
// param: key string
// param: start stop int 排名区间
func (o *ORM) ZRemRangeByRank(key string, start, stop int) *ORM {
	o.query.ZRemRangeByRank(key, start, stop)
	return o
}

// ZCard 返回有序集成员个数
// param: key string
func (o *ORM) ZCard(key string) *ORM {
	o.query.ZCard(key)
	return o
}

// ZScore 返回有序集中，成员的分数值。
// param: key string
// param: member interface{} 成员
func (o *ORM) ZScore(key string, member interface{}) *ORM {
	o.query.ZScore(key, member)
	return o
}

// ZRank 返回有序集中指定成员的排名。其中有序集成员按分数值递增(从小到大)顺序排列。
// param: key string
// param: member interface{} 成员，任意类型
func (o *ORM) ZRank(key string, member interface{}) *ORM {
	o.query.ZRank(key, member)
	return o
}

// ZRevRank 返回有序集中指定成员的排名。其中有序集成员按分数值递增(从大到小)顺序排列。
// param: key string
// param: member interface{} 成员，任意类型
func (o *ORM) ZRevRank(key string, member interface{}) *ORM {
	o.query.ZRevRank(key, member)
	return o
}

// ZCount 计算有序集合中指定分数区间的成员数量
// param: key string
// param: min interface{}
// param: max interface{}
func (o *ORM) ZCount(key string, min, max interface{}) *ORM {
	o.query.ZCount(key, min, max)
	return o
}

// ZPopMin 移除并弹出有序集合中分值最小的 count 个元素
// redis v5.0.0+
// param: key string
// param: count ...int64 不设置count参数时，弹出一个元素
func (o *ORM) ZPopMin(key string, count ...int64) *ORM {
	o.query.ZPopMin(key, count...)
	return o
}

// ZPopMax 移除并弹出有序集合中分值最大的的 count 个元素
// redis v5.0.0+
// param: key string
// param: count ...int64 不设置count参数时，弹出一个元素
func (o *ORM) ZPopMax(key string, count ...int64) *ORM {
	o.query.ZPopMax(key, count...)
	return o
}

// ZIncrBy 对有序集合中指定成员的分数加上增量 increment，可以通过传递一个负数值 increment ，
// 让分数减去相应的值，比如 ZINCRBY key -5 member ，
// 就是让 member 的 score 值减去 5 。当 key 不存在，或分数不是 key 的成员时，
// ZINCRBY key increment member 等同于 ZADD key
// increment member 。当 key 不是有序集类型时，返回一个错误。分数值可以是整数值或双精度浮点数。
// param: key string
// param: member interface{} 任意类型数据
// param: incr interface{} 增量值，可以为整数或双精度浮点
func (o *ORM) ZIncrBy(key string, member, incr interface{}) *ORM {
	o.query.ZIncrBy(key, member, incr)
	return o
}

// ZRange 返回有序集中，指定区间内的成员。其中成员的位置按分数值递增(从小到大)来排序。
// param: key string
// param: int start, stop 以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，你也可以使用负数下标，
// param: withScore 是否返回有序集的分数， true - 返回，false - 不返回，默认不返回，结果分开在两个数组存储，但是数组下标是一一对应的，比如 member[3] 成员的分数是 score[3]
// 以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。
func (o *ORM) ZRange(key string, start, stop int, withScore ...bool) *ORM {
	o.query.ZRange(key, start, stop, withScore...)
	return o
}

// ZRangeByScore 根据分数返回有序集中指定区间的成员，顺序从小到大
// param: key string
// param: int min, max 分数的范围，类型必须为 int, float，但是 -inf +inf 表示负正无穷大
// param: withScores 是否返回有序集的分数， true - 返回，false - 不返回，默认不返回，结果分开在两个数组存储，但是数组下标是一一对应的，比如 member[3] 成员的分数是 score[3]
// param: limit offset count 游标
func (o *ORM) ZRangeByScore(key string, min, max interface{}, withScores bool, limit ...int64) *ORM {
	o.query.ZRangeByScore(key, min, max, withScores, limit...)
	return o
}

// ZRevRange 返回有序集中指定区间的成员，其中成员的位置按分数值递减(从大到小)来排列。
// param: key string
// param: start, stop 排名区间，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，你也可以使用负数下标，
// param: withScore 是否返回有序集的分数， true - 返回，false - 不返回，默认不返回，结果分开在两个数组存储，但是数组下标是一一对应的，比如 member[3] 成员的分数是 score[3]
// 以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。
func (o *ORM) ZRevRange(key string, start, stop int, withScore ...bool) *ORM {
	o.query.ZRevRange(key, start, stop, withScore...)
	return o
}

// ZRevRangeByScore 返回有序集中指定分数区间内的所有的成员。有序集成员按分数值递减(从大到小)的次序排列。
// param: key string
// param: max, min  interface{} 分数区间，类型为整数或双精度浮点数，但是 -inf +inf 表示负正无穷大
// param: withScore 是否返回有序集的分数， true - 返回，false - 不返回，默认不返回，结果分开在两个数组存储，但是数组下标是一一对应的，比如 member[3] 成员的分数是 score[3]
// param: limit offset count 游标
func (o *ORM) ZRevRangeByScore(key string, max, min interface{}, withScore bool, limit ...int64) *ORM {
	o.query.ZRevRangeByScore(key, max, min, withScore, limit...)
	return o
}
