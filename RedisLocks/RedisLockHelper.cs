using Newtonsoft.Json;
using SIE.Caches;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace SIE.RedisLock.RedisLocks
{
    /// <summary>
    /// StackExchange.Redis的分布式锁类
    /// </summary>
    public class RedisLockHelper : RedisLockAbstractHelper
    {
        /// <summary>
        /// RedisCache
        /// </summary>
        public RedisCache RedisCache => AppRuntime.Service.Resolve<CacheService>().DistributedCache as RedisCache;

        /// <summary>
        /// Lua语句：当不存在时加锁，如果失败直接将已加锁的解锁并返回结果
        /// 2023-05-05 可重入
        /// </summary>
        public const string LockLuaStript =
              @"local lockCount = 0;
                local num=table.getn(KEYS);
                local threadId = ARGV[1];
                local releaseTime = ARGV[2];

                for i=1,num do 
                    local key = KEYS[i]; -- 第1个参数,锁的key
                    --local threadId = ARGV[i]; -- 第2个参数,线程唯一标识
                    --local releaseTime = ARGV[i+num]; -- 第3个参数,锁的自动释放时间

                    -- 判断锁是否已存在
                    if redis.call('exists',key)==0 then
                        redis.call('hset',key,threadId,'1'); -- 不存在, 则获取锁
                        redis.call('pexpire',key,releaseTime); -- 设置有效期
                        lockCount = lockCount + 1; -- 结果+1
                    else
                        -- 锁已存在
                        -- 判断threadId是否是自己
                        if redis.call('hexists',key,threadId)==1 then                        
                            redis.call('hincrby',key,threadId,1); -- 如果是自己，则重入次数+1
                            redis.call('pexpire',key,releaseTime); -- 设置有效期
                            lockCount= lockCount + 1;-- 结果+1
                        else
                            -- 代码走到这里,说明获取锁的不是自己，获取锁失败
                            break;
                        end;
                    end;
                end 
                --判断是否存在失败
                if lockCount == num
                then
                    return 0;
                else
                    --失败则回滚已加锁数据
                    for i=1,lockCount do
                        redis.call('del', KEYS[i]);
                    end
                    return lockCount+1;
                end";


        /// <summary>
        /// Lua语句：当值相等时才进行删除，防止将其他线程上的锁解掉了
        /// </summary>
        public const string UnLockLuaStript =
             @"for i=1,table.getn(KEYS) do 
                    -- 锁的 key
                    local key = KEYS[i];
                    -- 线程唯一标识
                    local threadId = ARGV[i];
                    -- 判断当前锁是否还是被自己持有
                    if (redis.call('hexists', key, threadId) > 0) then
                        -- 是自己的锁，则重入次数减一
                        local count = redis.call('hincrby', key, threadId, -1);
                         -- 判断重入次数是否已为0
                        if (count == 0) then
                            --  等于 0，说明可以释放锁，直接删除
                            redis.call('del', key);
                        end;
                    end; 
              end 
              return 0";

        /// <summary>
        /// Lua语句：当值相等时才进行续期，防止将其他线程上的锁解掉了
        /// </summary>
        public const string ExpireLockLuaStript =
              @"local num=table.getn(KEYS);
                local expireCount = 0;
                for i=1,num do 
                    if (redis.call('hexists', KEYS[i], ARGV[i])>0) then
                        redis.call('pexpire',KEYS[i],ARGV[i+num]);
                        expireCount= expireCount + 1;-- 结果+1
                    end 
                end 
                return expireCount";

        /// <summary>
        /// 上锁
        /// </summary>
        /// <param name="lockKey">锁集合</param>
        /// <param name="lockValue">锁值</param>
        /// <param name="lockTime">锁时间</param>
        /// <returns>结果</returns>
        protected override int LockByCache(List<string> lockKey, string lockValue, double lockTime)
        {
            var ret = RedisCache.RunLua(LockLuaStript, lockKey.ToArray(), new string[2] { lockValue, lockTime.ToString() });
            return int.Parse(ret.ToString());
        }

        /// <summary>
        /// 解锁
        /// </summary>
        /// <param name="redisLocker"></param>
        public override void UnLock(params RedisLocker[] redisLocker)
        {
            if (redisLocker==null || redisLocker.Length==0)
                return;
            var lockKeyList = new List<string>(redisLocker.Length);
            var lockValueList = new List<string>(redisLocker.Length);
            for (int i = 0; i < redisLocker.Length; i++)
            {
                lockKeyList.Add(redisLocker[i].Key);
                lockValueList.Add(redisLocker[i].Value);
            }

            //lua脚本，也可以通过流形式加载进来

            //释放锁的redis脚本
            //var result=
            RedisCache.RunLua(UnLockLuaStript, lockKeyList.ToArray(), lockValueList.ToArray());
            //Debug.WriteLine(result);
        }

        /// <summary>
        /// 锁续期
        /// </summary>
        /// <param name="lockTime"></param>
        /// <param name="lockers"></param>
        public override bool ExpireLock(TimeSpan lockTime, params RedisLocker[] lockers)
        {
            if (lockers==null || lockers.Length==0)
                return false;
            var lockKeyList = new List<string>(lockers.Length);
            var lockValueList = new List<string>(lockers.Length);
            var lockTimeList = new List<string>(lockers.Length);
            var lockTimePx = lockTime.TotalMilliseconds.ToString();
            for (int i = 0; i < lockers.Length; i++)
            {
                //lockKey[i]=GetLockKey(lockKey[i]);
                //生成随机数
                //string lockValue = $"'{Guid.NewGuid().ToString("N")}'";
                lockKeyList.Add(lockers[i].Key);
                lockValueList.Add(lockers[i].Value);
                lockTimeList.Add(lockTimePx);
            }

            //lua脚本，也可以通过流形式加载进来
            //锁续期的redis脚本
            var result = RedisCache.RunLua(ExpireLockLuaStript, lockKeyList.ToArray(), lockValueList.Concat(lockTimeList).ToArray());
            return int.Parse(result.ToString())>0;
        }

    }

    /// <summary>
    /// StackExchange.Redis拓展类
    /// </summary>
    public static class RedisCacheEx
    {
        private static IDatabase redisDB { get; set; }

        private static object locker = new object();

        /// <summary>
        /// 初始化属性
        /// </summary>
        /// <param name="distributedCache"></param>
        private static void InitRedisDB(RedisCache distributedCache)
        {
            if (redisDB==null)
            {
                lock (locker)
                {
                    if (redisDB!=null)
                        return;
                    var propertyName = "DB";
                    var InstanceBindFlags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
                    var pro = typeof(RedisCache).GetProperty(propertyName, InstanceBindFlags);
                    object dbObj = pro.GetValue(distributedCache);
                    redisDB= dbObj as IDatabase;
                }
            }
        }


        /// <summary>
        /// 执行LUA语句
        /// </summary>
        /// <param name="distributedCache"></param>
        /// <param name="script"></param>
        /// <param name="keys"></param>
        /// <param name="values"></param>
        public static RedisResult RunLua(this RedisCache distributedCache, string script, string[] keys, string[] values)
        {
            InitRedisDB(distributedCache);

            RedisKey[] redisKeys = new RedisKey[keys.Length];
            for (int i = 0; i<keys.Length; i++)
            {
                redisKeys[i]=keys[i];
            }
            RedisValue[] redisValues = new RedisValue[values.Length];
            for (int i = 0; i<values.Length; i++)
            {
                redisValues[i]=values[i];
            }
            return redisDB.ScriptEvaluate(script, redisKeys, redisValues, CommandFlags.DemandMaster);
        }

        /// <summary>
        /// 批量添加Set类型
        /// </summary>
        /// <param name="distributedCache"></param>
        /// <param name="key"></param>
        /// <param name="values"></param>
        /// <param name="timeSpan"></param>
        public static long AddSetRange(this RedisCache distributedCache, string key, IEnumerable<string> values, TimeSpan timeSpan)
        {
            InitRedisDB(distributedCache);
            distributedCache.KeyExpire(key, timeSpan);
            RedisValue[] valueList = values.Select(u => (RedisValue)u).ToArray();
            long ret = redisDB.SetAdd(key, valueList, CommandFlags.DemandMaster); ;
            return ret;
        }

        /// <summary>
        /// 获取Set集合，并移除
        /// </summary>
        /// <param name="distributedCache"></param>
        /// <param name="key"></param>
        public static List<string> GetSetAndRemove(this RedisCache distributedCache, string key)
        {
            InitRedisDB(distributedCache);
            RedisValue[] valueList = redisDB.SetMembers(key); ;
            redisDB.KeyDelete(key);
            var list = valueList.Select(p => p.ToString()).ToList();
            return list;
        }

        /// <summary>
        /// 获取Set集合
        /// </summary>
        /// <param name="distributedCache"></param>
        /// <param name="key"></param>
        public static List<string> GetSet(this RedisCache distributedCache, string key)
        {
            InitRedisDB(distributedCache);
            RedisValue[] valueList = redisDB.SetMembers(key); 
            var list = valueList.Select(p => p.ToString()).ToList();
            return list;
        }

        /// <summary>
        /// 记录Hash
        /// </summary>
        /// <param name="distributedCache"></param>
        /// <param name="key"></param>
        /// <param name="hashValues"></param>
        /// <param name="timeSpan"></param>
        public static bool AddHash(this RedisCache distributedCache, string key, Dictionary<string, string> hashValues,
            TimeSpan timeSpan)
        {
            InitRedisDB(distributedCache);
            HashEntry[] entries = new HashEntry[hashValues.Count];
            var keys = hashValues.Keys.ToArray();
            var values = hashValues.Values.ToArray();
            for (int i = 0; i< hashValues.Count; i++)
            {
                entries[i]= new HashEntry(keys[i], values[i]);
            };
            distributedCache.KeyExpire(key, timeSpan);
            redisDB.HashSet(key, entries, CommandFlags.DemandMaster);
            return true;
        }

        /// <summary>
        /// 记录Hash
        /// </summary>
        /// <param name="distributedCache"></param>
        /// <param name="key"></param>
        /// <param name="hashValues"></param>
        /// <param name="timeSpan"></param>
        public static bool AddHash<TKey, TValue>(this RedisCache distributedCache, string key, Dictionary<TKey, TValue> hashValues,
            TimeSpan timeSpan)
        {
            InitRedisDB(distributedCache);
            var keys = hashValues.Keys.Select(p => JsonConvert.SerializeObject(p)).ToArray();
            var values = hashValues.Values.Select(p=>JsonConvert.SerializeObject(p)).ToArray();
            HashEntry[] entries = hashValues.Select(
                p => new HashEntry(
                    JsonConvert.SerializeObject(p.Key), 
                    JsonConvert.SerializeObject(p.Value))
                ).ToArray();
            distributedCache.KeyExpire(key, timeSpan);
            redisDB.HashSet(key, entries, CommandFlags.DemandMaster);
            return true;
        }

        /// <summary>
        /// 获取Hash
        /// </summary>
        /// <param name="distributedCache"></param>
        /// <param name="key"></param>
        public static Dictionary<string, string> GetHash(this RedisCache distributedCache, string key)
        {
            InitRedisDB(distributedCache);
            var hashValues = redisDB.HashGetAll(key, CommandFlags.DemandMaster);
            var dic = hashValues.ToDictionary(p=>p.Name.ToString(),p=>p.Value.ToString());
            return dic;
        }

        /// <summary>
        /// 记录Hash
        /// </summary>
        /// <param name="distributedCache"></param>
        /// <param name="key"></param>
        public static Dictionary<TKey, TValue> GetHash<TKey,TValue>(this RedisCache distributedCache, string key)
        {
            InitRedisDB(distributedCache);
            var hashValues = redisDB.HashGetAll(key, CommandFlags.DemandMaster);
            var dic = hashValues.ToDictionary(
                p => JsonConvert.DeserializeObject<TKey>(p.Name),
                p => JsonConvert.DeserializeObject<TValue>(p.Value));
            return dic;
        }



        /// <summary>
        /// 设置Key的时间
        /// </summary>
        /// <param name="distributedCache"></param>
        /// <param name="key">redis key</param>
        /// <param name="expiry"></param>
        /// <returns></returns>
        public static bool KeyExpire(this RedisCache distributedCache, string key, TimeSpan? expiry)
        {
            InitRedisDB(distributedCache);
            return redisDB.KeyExpire(key, expiry, CommandFlags.DemandMaster);
        }

    }
}
