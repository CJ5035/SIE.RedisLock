using SIE.Caches;
using System;
using System.Collections.Generic;

namespace SIE.RedisLock.RedisLocks
{
    /// <summary>
    /// CSRedisCore Redis的分布式锁类
    /// </summary>
    public class CSRedisLockHelper : RedisLockAbstractHelper
    {
        /// <summary>
        /// CSRedisCache
        /// </summary>
        public CSRedisCache CSRedisCache =>
            RT.Service.Resolve<CacheService>().DistributedCache as CSRedisCache;
        /// <summary>
        /// 获取锁的脚本
        /// </summary>
        public const string LockLuaStript =
              @"local lockCount = 0;
                local num=tonumber(KEYS[1]);
                local threadId=ARGV[1+num]; -- 第2个参数,线程唯一标识
                local releaseTime=ARGV[2+num]; -- 第3个参数,锁的自动释放时间
                for i=1,num do 
                    local key = ARGV[i]; -- 第1个参数,锁的key
                    
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
                        redis.call('del', ARGV[i]);
                    end
                    return lockCount+1;
                end";

       
        /// <summary>
        /// 当值相等时才进行删除，防止将其他线程上的锁解掉了
        /// </summary>
        public const string UnLockLuaStript =
             @" local num=tonumber(KEYS[1]);
                for i=1,num do 
                    -- 锁的 key
                    local key = ARGV[i];
                    -- 线程唯一标识
                    local threadId = ARGV[i+num];
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
        /// 当值相等时才进行续期，防止将其他线程上的锁解掉了
        /// </summary>
        public const string ExpireLockLuaStript =
              @"local num=tonumber(KEYS[1]);
                local expireCount = 0;
                for i=1,num do 
                    if redis.call('hexists',ARGV[i],ARGV[i+num])>0 then
                        redis.call('pexpire',ARGV[i],ARGV[i+num+num])
                        expireCount= expireCount + 1;-- 结果+1  
                    end 
                end 
                return expireCount";

        /// <summary>
        /// 上锁
        /// </summary>
        /// <param name="lockKey">锁集合</param>
        /// <param name="lockValueList">随机数集合</param>
        /// <param name="lockTimeList">锁时间</param>
        /// <returns>结果</returns>
        protected override int LockByCache(List<string> lockKey, string lockValueList, double lockTimeList)
        {
            int count = lockKey.Count;
            List<object> keys = new List<object>(count*3);
            keys.AddRange(lockKey);
            keys.Add(lockValueList);
            keys.Add(lockTimeList.ToString());
            object[] objs= keys.ToArray();
            var result= CSRedisCache.DB.Eval(LockLuaStript, count.ToString(), objs);
            return int.Parse(result.ToString());
        }

        /// <summary>
        /// 锁续期
        /// </summary>
        /// <param name="lockTime"></param>
        /// <param name="lockers"></param>
        public override bool ExpireLock(TimeSpan lockTime, params RedisLocker[] lockers)
        {
            int count = lockers.Length;
            List<object> keys = new List<object>(lockers.Length*3);
            List<object> values = new List<object>(lockers.Length);
            List<object> times = new List<object>(lockers.Length);
            string time = lockTime.TotalMilliseconds.ToString();
            foreach (RedisLocker locker in lockers)
            {
                keys.Add(locker.Key);
                values.Add(locker.Value);
                times.Add(time);
            }
            keys.AddRange(values);
            keys.AddRange(times);
            object[] objs = keys.ToArray();
            var result = CSRedisCache.DB.Eval(ExpireLockLuaStript, count.ToString(), objs);            
            return (long)result > 0;
        }

        /// <summary>
        /// 解锁
        /// </summary>
        /// <param name="redisLocker"></param>
        public override void UnLock(params RedisLocker[] redisLocker)
        {
            int count = redisLocker.Length;
            List<object> keys = new List<object>(redisLocker.Length*2);
            List<object> values = new List<object>(redisLocker.Length);
            foreach (RedisLocker locker in redisLocker)
            {
                keys.Add(locker.Key);
                values.Add(locker.Value);
            }
            keys.AddRange(values);
            object[] objs = keys.ToArray();
            var result = CSRedisCache.DB.Eval(UnLockLuaStript, count.ToString(), objs);
            //Debug.WriteLine(result);
        }

        
    }
}
