using System;
using System.Collections.Generic;
using System.Text;

namespace SIE.RedisLock.RedisLocks
{
    /// <summary>
    /// 
    /// </summary>
    public class EmptyLockHelper : RedisLockAbstractHelper
    {
        string expString = "无法加载Redis锁实现类，请检查是否WPF使用Redis锁 或 Redis配置EnableDistributedCache没有设置为true";
        public override bool ExpireLock(TimeSpan lockTime, params RedisLocker[] lockers)
        {
            throw new Exception(expString);
        }

        //public override RedisLockerContainer Lock(TimeSpan lockTime, string errMsg = "", params string[] lockKey)
        //{
        //    throw new Exception(expString);
        //}

        public override void UnLock(params RedisLocker[] redisLocker)
        {
            throw new Exception(expString);
        }

        protected override int LockByCache(List<string> lockKey, string lockValue, double msTime)
        {
            throw new Exception(expString);
        }
    }
}
