using CSRedis;
using System;
using System.Reflection;

namespace SIE.RedisLock.RedisLocks
{
    /// <summary>
    /// CsRedis锁扩展
    /// </summary>
    public static class CsRedisLockEx
    {
        /// <summary>
        /// 开启分布式锁，若超时返回null
        /// 拓展RedisClient的Lock方法，不进行等待，获取到锁立即返回，若获取不到锁，直接返回null
        /// </summary>
        /// <param name="redisClient"></param>
        /// <param name="name">锁名称</param>
        /// <param name="timeoutSeconds">超时（秒）</param>
        /// <param name="autoDelay">自动延长锁超时时间，看门狗线程的超时时间为timeoutSeconds/2 ， 在看门狗线程超时时间时自动延长锁的时间为timeoutSeconds。除非程序意外退出，否则永不超时</param>
        /// <returns></returns>
        public static CSRedisClientLock LockWithoutWait(this CSRedisClient redisClient, string name, int timeoutSeconds, bool autoDelay = true)
        {
            /*使用示例：
             * var csRedisCache = RT.Service.Resolve<CacheService>().DistributedCache as CSRedisCache;
            using (var locker = csRedisCache.DB.LockWithoutWait("ZX-调拨单上传ERP_" + RT.InvOrg, 600))
             */
            name = "CSRedisClientLock:" + name;
            string value = Guid.NewGuid().ToString();
            if (redisClient.Set(name, value, timeoutSeconds, RedisExistence.Nx))
            {
                double refreshSeconds = timeoutSeconds / 2.0;
                //升级到3.8.8 可用
                //return new CSRedisClientLock(redisClient, name, value, timeoutSeconds, refreshSeconds, autoDelay);

                //未升级到3.8.8前，无法使用私有构造函数，只能使用Activator.CreateInstance
                return Activator.CreateInstance(
                    typeof(CSRedisClientLock), BindingFlags.Instance|BindingFlags.NonPublic, null, 
                    new object[] { redisClient, name, value, timeoutSeconds, refreshSeconds, autoDelay }, null) 
                    as CSRedisClientLock;
            }
            return null;
        }
    }
}
