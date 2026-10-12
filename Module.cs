using SIE.Caches;
using SIE.Modules;
using SIE.RedisLock;
using SIE.RedisLock.RedisLocks;
using System.Diagnostics;

[assembly: Module(typeof(Module))]
namespace SIE.RedisLock
{
    /// <summary>
    /// 模块
    /// </summary>
    public class Module : DomainModule
    {
        /// <summary>
        /// 模块初始化
        /// </summary>
        /// <param name="app"></param>
        public override void Initialize(IApp app)
        {
            // 模块初始化完成事件
            app.AppMetaCompleted += App_AppMetaCompleted;
        }

        private void App_AppMetaCompleted(object sender, System.EventArgs e)
        {
            if (RT.IsOnClient())
            {
                CSRedisCacheOptions redisConfig = AppRuntime.Config.Get<CSRedisCacheOptions>("RedisConnectionStrings");
                CacheService cacheService = AppRuntime.Service.Resolve<CacheService>();
                if (redisConfig == null || !redisConfig.EnableDistributedCache)
                {
                    RedisCacheOptions redisCacheOptions = AppRuntime.Config.Get<RedisCacheOptions>("RedisCache");
                    if (redisCacheOptions == null || string.IsNullOrEmpty(redisCacheOptions.Configuration) || !redisCacheOptions.EnableDistributedCache)
                        cacheService.SetCache((IMemoryCache)new InMemoryCache(), (IDistributedCache)new InMemoryCache());
                    else
                        cacheService.SetCache((IMemoryCache)new InMemoryCache(), (IDistributedCache)new RedisCache(redisCacheOptions.Configuration));
                }
                else
                {
                    CSRedisCache instnace = new CSRedisCache(redisConfig);
                    cacheService.SetCache((IMemoryCache)new InMemoryCache(), (IDistributedCache)instnace);
                }
            }


            // 注册锁工具
            if (RT.Service.Resolve<Caches.CacheService>().DistributedCache is CSRedisCache)
                RT.Service.Register<RedisLockAbstractHelper, CSRedisLockHelper>();
            else if (RT.Service.Resolve<Caches.CacheService>().DistributedCache is RedisCache)
                RT.Service.Register<RedisLockAbstractHelper, RedisLockHelper>();
            else
                RT.Service.Register<RedisLockAbstractHelper, EmptyLockHelper>();
        }
    }
}
