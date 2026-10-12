using SIE.Domain.Validation;
using SIE.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SIE.RedisLock.RedisLocks
{
    /// <summary>
    /// 分布式锁帮助类基类，需要重写LockByCache与UnLock
    /// </summary>
    public abstract class RedisLockAbstractHelper
    {
        /// <summary>
        /// 进程实例标识，用于区分不同进程的锁持有者
        /// </summary>
        private static readonly string ProcessInstanceId = Guid.NewGuid().ToString("N");

        /// <summary>
        /// 锁前缀，与普通缓存区分开
        /// </summary>
        protected virtual string LockKeyPrefix => "Lock:";

        /// <summary>
        /// 获取锁失败提示
        /// </summary>
        public string Tip { get; set; } = "任务进行中，请等待30秒后重试";

        /// <summary>
        /// 将数据添加锁前缀
        /// </summary>
        /// <param name="lockKey"></param>
        /// <returns></returns>
        public virtual string GetLockKey(string lockKey)
        {
            return $"{LockKeyPrefix}{lockKey}";
        }
        /// <summary>
        /// 加锁并进行续期
        /// </summary>
        /// <param name="lockTime">上锁时长</param>
        /// <param name="lockKey">上锁目标</param>
        /// <returns></returns>
        public virtual RedisLockerContainer Lock(TimeSpan lockTime, params string[] lockKey)
            => Lock(lockTime, lockKey.ToList());

        /// <summary>
        /// 加锁并进行续期（带重试功能）
        /// </summary>
        /// <param name="lockTime">上锁时长</param>
        /// <param name="waitTime">失败重试等待时间</param>
        /// <param name="lockKey">上锁目标</param>
        /// <returns></returns>
        public virtual RedisLockerContainer LockWait(TimeSpan lockTime, TimeSpan waitTime, params string[] lockKey)
            => LockWait(lockTime, waitTime, string.Empty, lockKey.ToArray());

        /// <summary>
        /// 加锁并进行续期
        /// </summary>
        /// <param name="lockTime">上锁时长</param>
        /// <param name="lockKey">上锁目标</param>
        /// <param name="errMsg">错误提示,不输入显示"获取资源失败"</param>
        /// <returns></returns>
        public virtual RedisLockerContainer Lock(TimeSpan lockTime, List<string> lockKey, string errMsg = "")
            => LockWait(lockTime, TimeSpan.Zero, errMsg, lockKey.ToArray());

        /// <summary>
        /// 加锁并进行续期（带重试功能）
        /// </summary>
        /// <param name="lockTime">上锁时长</param>
        /// <param name="waitTime">失败重试等待时间</param>
        /// <param name="lockKey">上锁目标</param>
        /// <param name="errMsg">错误提示,不输入显示"获取资源失败"</param>
        /// <returns></returns>
        public virtual RedisLockerContainer LockWait(TimeSpan lockTime, TimeSpan waitTime, List<string> lockKey, string errMsg = "")
            => LockWait(lockTime, waitTime, errMsg, lockKey.ToArray());

        /// <summary>
        /// 上锁并等待
        /// </summary>
        /// <param name="lockTime">锁过期时间</param>
        /// <param name="waitTime">等待时间，=0则为不等待</param>
        /// <param name="errMsg">错误提示</param>
        /// <param name="lockKey">锁Key</param>
        /// <returns>锁对象</returns>
        public virtual RedisLockerContainer LockWait(TimeSpan lockTime, TimeSpan waitTime, string errMsg, params string[] lockKey)
        {
            if (lockKey == null || lockKey.Length  <= 0)
                throw new ValidationException($"{DateTime.Now.ToString("G")}\r\n获取锁{lockKey}失败，锁对象不能为空");

            //用进程实例ID+线程ID，让锁有同线程可重入机制
            var lockValue = $"{ProcessInstanceId}-{Thread.CurrentThread.ManagedThreadId}";

            var newlockKey = lockKey.Select(GetLockKey).ToArray();
            //上锁，返回上锁失败的锁对象序号,如果返回0则表示全部上锁成功
            var ret = LockWaitByCache(newlockKey.ToList(), lockValue, lockTime.TotalMilliseconds, waitTime.TotalMilliseconds);

            if (ret > 0)
            {
                errMsg = errMsg.IsNotEmpty() ? errMsg.L10nFormat(lockKey[ret-1])
                    : $"{DateTime.Now.ToString("G")}\r\n[{lockKey[ret-1]}]{Tip}";
                throw new ValidationException(errMsg);
            }

            //创建锁对象
            var redisLockers = newlockKey.Select(t => new RedisLocker(this, null, t, lockValue)).ToList();

            var locker = new RedisLockerContainer(redisLockers);
            //开启续期线程
            CreateRenewalLockerTask(lockTime, redisLockers.ToArray());
            return locker;
        }

        /// <summary>
        /// 加锁(阻塞)
        /// </summary>
        /// <param name="lockKey"></param>
        /// <param name="lockValue"></param>
        /// <param name="msTime"></param>
        /// <param name="waitTime"></param>
        /// <returns></returns>
        protected int LockWaitByCache(List<string> lockKey, string lockValue, double msTime, double waitTime = 0)
        {
            //如果没有传重试时间，则不进行等待
            if (waitTime == 0)
                return LockByCache(lockKey.ToList(), lockValue, msTime);

            //上锁，返回上锁失败的锁对象序号
            DateTime now = DateTime.Now;
            int ret = 0;
            while (DateTime.Now.Subtract(now).TotalMilliseconds < waitTime)
            {
                ret = LockByCache(lockKey.ToList(), lockValue, msTime);
                if (ret == 0) break;
                Thread.CurrentThread.Join(3);
            }
            return ret;
        }


        /// <summary>
        /// 加锁（需要重写）
        /// </summary>
        /// <param name="lockKey"></param>
        /// <param name="lockValue"></param>
        /// <param name="msTime"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        protected abstract int LockByCache(List<string> lockKey, string lockValue, double msTime);


        /// <summary>
        /// 解锁
        /// </summary>
        /// <param name="redisLocker"></param>
        /// <exception cref="NotImplementedException"></exception>
        public abstract void UnLock(params RedisLocker[] redisLocker);


        /// <summary>
        /// 续期命令
        /// </summary>
        /// <param name="lockTime"></param>
        /// <param name="lockers"></param>
        /// <exception cref="NotImplementedException"></exception>
        public abstract bool ExpireLock(TimeSpan lockTime, params RedisLocker[] lockers);

        /// <summary>
        /// 创建锁续期线程（只需对ExpireLock进行重写后调用本方法）
        /// </summary>
        /// <param name="lockTime">上锁时长</param>
        /// <param name="redisLockers">RedisLocker集合</param>
        protected virtual Task CreateRenewalLockerTask(TimeSpan lockTime, params RedisLocker[] redisLockers)
        {
            var cts = new CancellationTokenSource();
            //取消Task的令牌
            var token = cts.Token;
            //续期时间间隔
            var taskWaitTime = TimeSpan.FromMilliseconds(lockTime.TotalMilliseconds / 3);
            foreach (var redisLocker in redisLockers)
            {
                redisLocker.TaskCts = cts;
            }

            return Task.Run(() =>
            {
                var lockers = redisLockers.ToList();
                var lockerKeyStr = "执行续期：" + string.Join(",", lockers.Select(l => l.Key));
                //失败次数
                int failCount = 0;
                while (true)
                {
                    Thread.Sleep(taskWaitTime);
                    if (token.IsCancellationRequested)
                        return;//return是直接跳出整个Task.Run
                    RT.Logger.Info(lockerKeyStr);
                    if (!ExpireLock(lockTime, lockers.ToArray()))
                    {
                        failCount++;
                        if (failCount >= 2)
                            break;//break是跳出当前循环
                    }
                }
            }, token);
        }
    }


    /// <summary>
    /// 分布式缓存锁，通常由SieRedisLockHelper进行创建
    /// </summary>
    public class RedisLocker : IDisposable
    {
        /// <summary>
        /// 返回锁对象
        /// </summary>
        /// <param name="redisLockHelper">上锁的RedisHelper</param>
        /// <param name="cancellationTokenSource">续期线程的取消令牌</param>
        /// <param name="key">锁名</param>
        /// <param name="value">锁值</param>
        public RedisLocker(RedisLockAbstractHelper redisLockHelper, CancellationTokenSource cancellationTokenSource, string key, string value)
        {
            TaskCts = cancellationTokenSource;
            this.redisLockHelper = redisLockHelper;
            Key = key;
            Value = value;
        }
        /// <summary>
        /// redis锁执行对象
        /// </summary>
        public RedisLockAbstractHelper redisLockHelper { get; protected set; }
        /// <summary>
        /// 续期线程令牌
        /// </summary>
        public CancellationTokenSource TaskCts { get; set; }
        /// <summary>
        /// Locker的Key
        /// </summary>
        public string Key { get; set; }
        /// <summary>
        /// Locker的值
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// 锁释放
        /// </summary>
        public void Dispose()
        {
            redisLockHelper.UnLock(this);
        }
    }

    /// <summary>
    /// 分布式缓存锁集合容器，通常由SieRedisLockHelper进行创建
    /// </summary>
    public class RedisLockerContainer : IDisposable
    {
        private RedisLockAbstractHelper RedisLockHelper => this.lockers.FirstOrDefault()?.redisLockHelper;

        /// <summary>
        /// 续期线程的关闭节点
        /// </summary>
        private List<CancellationTokenSource> TaskCts => this.lockers.Select(p => p.TaskCts).Distinct().ToList();

        /// <summary>
        /// 分布式锁集合
        /// </summary>
        public List<RedisLocker> lockers { get; set; }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="lockers"></param>
        public RedisLockerContainer(List<RedisLocker> lockers)
        {
            this.lockers = lockers;
        }

        /// <summary>
        /// 锁释放
        /// </summary>
        public void Dispose()
        {
            if (this.TaskCts!=null)
            {
                foreach (var cts in this.TaskCts)
                {
                    if (!cts.IsCancellationRequested)
                        cts.Cancel();
                }
            }
            RedisLockHelper.UnLock(this.lockers.ToArray());
        }
    }
}
