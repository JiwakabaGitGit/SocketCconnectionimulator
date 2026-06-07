// =========================================================================
// ServerProxy - 任意テキスト送受信 TCP サーバ (.NET Framework 4.8)
//
// JSON-RPC ではなく、任意のテキストメッセージを CR+LF (\r\n) 区切りで
// 送受信する TCP サーバ。外部クライアントからの接続を受け付ける。
//
// 用途: 実機やシミュレータとのテキストベース通信の中継・テスト。
//
// 【仕様】
//   メッセージ区切り : CR+LF (\r\n)
//   接続数           : 複数クライアント同時接続対応
//   送信             : 送信元クライアントへの返信（SendToSenderAsync）
//                      または全クライアントへのブロードキャスト（SendToAllAsync）
//   受信             : MessageReceived イベントで通知（送信用コールバック付き）
//   バッファ上限     : 1 メッセージあたり 1 MB
// =========================================================================
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SocketApp.Models
{
    /// <summary>
    /// CR+LF 区切りの任意テキストを送受信する TCP サーバ。
    /// </summary>
    public class ServerProxy : IDisposable
    {
        // ---- 定数 ----

        /// <summary>受信バッファの最大蓄積サイズ（1 MB）。超過時はクライアントを切断する。</summary>
        private const int MaxMessageBufferBytes = 1 * 1024 * 1024;

        // ---- フィールド ----

        private TcpListener _listener;
        private CancellationTokenSource _cts;

        /// <summary>接続中クライアントの管理セット。_lock で保護する。</summary>
        private readonly HashSet<TcpClient> _connectedClients = new HashSet<TcpClient>();
        private readonly object _lock = new object();

        private bool _disposed = false;

        // ---- プロパティ ----

        /// <summary>リッスンポート番号</summary>
        public int Port { get; }

        /// <summary>サーバが起動中かどうか</summary>
        public bool IsRunning { get; private set; }

        /// <summary>現在の接続クライアント数</summary>
        public int ConnectedClientCount
        {
            get { lock (_lock) { return _connectedClients.Count; } }
        }

        // ---- イベント ----

        /// <summary>ログメッセージ通知（メッセージ, isError）</summary>
        public event Action<string, bool> LogMessage;

        /// <summary>起動/停止状態の変化通知（isRunning）</summary>
        public event Action<bool> StateChanged;

        /// <summary>
        /// クライアントからテキストメッセージを受信したとき発火する。
        /// 引数:
        ///   message      … 受信したメッセージ文字列
        ///   replySender  … 送信元クライアントへ返信する非同期メソッド
        ///                  （引数: 返信テキスト）
        /// 使用例:
        ///   proxy.MessageReceived += async (msg, reply) =>
        ///   {
        ///       await reply("ACK: " + msg);
        ///   };
        /// </summary>
        public event Func<string, Func<string, Task>, Task> MessageReceived;

        /// <summary>クライアント接続数が変化したとき発火する</summary>
        public event Action<int> ClientCountChanged;

        // ---- コンストラクタ ----

        /// <summary>
        /// ServerProxy を初期化する。
        /// </summary>
        /// <param name="port">リッスンするポート番号</param>
        public ServerProxy(int port)
        {
            Port = port;
        }

        // =================================================================
        // 起動 / 停止
        // =================================================================

        /// <summary>サーバを起動し、クライアントの接続受付を開始する</summary>
        public void Start()
        {
            if (IsRunning) return;

            _cts = new CancellationTokenSource();
            _listener = new TcpListener(IPAddress.Any, Port);
            _listener.Start();
            IsRunning = true;
            StateChanged?.Invoke(true);
            Log(string.Format("起動: ポート {0} でリッスン開始", Port));

            // タスクリーク防止: _ = で警告を抑制しつつ破棄
            _ = AcceptLoopAsync(_cts.Token);
        }

        /// <summary>サーバを停止し、全クライアントを切断する</summary>
        public void Stop()
        {
            if (!IsRunning) return;

            // キャンセルシグナルを送信
            _cts?.Cancel();

            // _listener.Stop() で AcceptTcpClientAsync が ObjectDisposedException をスローし
            // AcceptLoopAsync が終了する（.NET Framework 4.8 では AcceptTcpClientAsync に
            // CancellationToken オーバーロードが存在しないため、この方法でキャンセルする）
            _listener?.Stop();

            // 全クライアントを切断
            lock (_lock)
            {
                foreach (var c in _connectedClients)
                {
                    try { c.Close(); } catch { }
                }
                _connectedClients.Clear();
            }

            IsRunning = false;
            StateChanged?.Invoke(false);
            ClientCountChanged?.Invoke(0);

            // CancellationTokenSource を破棄して次回 Start() に備える
            _cts?.Dispose();
            _cts = null;

            Log("停止しました");
        }

        // =================================================================
        // 送信（送信元クライアントへの返信）
        // =================================================================

        /// <summary>
        /// 指定クライアントにテキストメッセージを送信する。
        /// メッセージ末尾に CR+LF を自動付与する。
        /// ※ 通常は MessageReceived イベントの replySender 引数経由で呼ぶ。
        /// </summary>
        /// <param name="client">送信先クライアント</param>
        /// <param name="message">送信するテキスト（CR+LF は自動付与）</param>
        public async Task SendToSenderAsync(TcpClient client, string message)
        {
            byte[] data = Encoding.UTF8.GetBytes(message + "\r\n");
            try
            {
                await client.GetStream().WriteAsync(data, 0, data.Length);
                Log(string.Format("送信（返信）: {0}", message));
            }
            catch (Exception ex)
            {
                Log(string.Format("送信エラー: {0}", ex.Message), isError: true);

                // 送信失敗 = 切断済みとみなしてリストから除去
                RemoveClient(client);
            }
        }

        // =================================================================
        // 送信（ブロードキャスト）
        // =================================================================

        /// <summary>
        /// 接続中の全クライアントにテキストメッセージを送信する。
        /// メッセージ末尾に CR+LF を自動付与する。
        /// 送信は並列実行し、失敗したクライアントはリストから除去する。
        /// </summary>
        /// <param name="message">送信するテキスト（CR+LF は自動付与）</param>
        public async Task SendToAllAsync(string message)
        {
            List<TcpClient> targets;
            lock (_lock) { targets = new List<TcpClient>(_connectedClients); }

            if (targets.Count == 0)
            {
                Log("送信スキップ（接続クライアントなし）", isError: true);
                return;
            }

            byte[] data = Encoding.UTF8.GetBytes(message + "\r\n");

            // 全クライアントへ並列送信（Task.WhenAll で待機）
            var tasks = new List<Task>(targets.Count);
            foreach (var client in targets)
            {
                tasks.Add(SendRawAsync(client, data));
            }
            await Task.WhenAll(tasks);

            Log(string.Format("送信（ブロードキャスト）: {0}", message));
        }

        /// <summary>生バイト列を送信する内部メソッド。失敗時はクライアントを除去する。</summary>
        private async Task SendRawAsync(TcpClient client, byte[] data)
        {
            try
            {
                await client.GetStream().WriteAsync(data, 0, data.Length);
            }
            catch (Exception ex)
            {
                Log(string.Format("送信エラー: {0}", ex.Message), isError: true);
                RemoveClient(client);
            }
        }

        // =================================================================
        // 接続受付ループ
        // =================================================================

        private async Task AcceptLoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    // .NET Framework 4.8 には CancellationToken 付き
                    // AcceptTcpClientAsync が存在しないため、
                    // Stop() 内の _listener.Stop() で ObjectDisposedException をスローさせて
                    // ループを終了する方式を採用する
                    client = await _listener.AcceptTcpClientAsync();
                }
                catch (ObjectDisposedException)
                {
                    // _listener.Stop() による正常終了
                    break;
                }
                catch (Exception ex) when (!ct.IsCancellationRequested)
                {
                    Log(string.Format("Accept エラー: {0}", ex.Message), isError: true);
                    continue;
                }
                catch
                {
                    // キャンセル済みの場合はそのまま抜ける
                    break;
                }

                lock (_lock) { _connectedClients.Add(client); }

                var ep = client.Client.RemoteEndPoint as IPEndPoint;
                Log(string.Format("クライアント接続: {0}", ep != null ? ep.ToString() : "不明"));
                ClientCountChanged?.Invoke(ConnectedClientCount);

                // クライアントごとに独立したタスクを起動
                // _ = でタスクリーク警告を抑制（例外は HandleClientAsync 内で処理）
                _ = HandleClientAsync(client, ct);
            }
        }

        // =================================================================
        // クライアント通信ハンドラ（CR+LF 区切り）
        // =================================================================

        /// <summary>
        /// 1クライアントとの通信を処理する。
        /// CR+LF (\r\n) 区切りでテキストメッセージを受信し、
        /// MessageReceived イベントを発火する。
        /// イベントハンドラには送信元への返信用コールバックを渡す。
        /// </summary>
        private async Task HandleClientAsync(TcpClient client, CancellationToken ct)
        {
            var stream = client.GetStream();
            var buffer = new byte[4096];
            var messageBuffer = new StringBuilder();

            // このクライアント専用の返信デリゲート
            Func<string, Task> replySender = reply => SendToSenderAsync(client, reply);

            try
            {
                while (!ct.IsCancellationRequested && client.Connected)
                {
                    int bytesRead;
                    try
                    {
                        bytesRead = await stream.ReadAsync(buffer, 0, buffer.Length, ct);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    if (bytesRead == 0) break; // 正常切断

                    // バッファ上限チェック（悪意あるクライアント対策）
                    if (messageBuffer.Length + bytesRead > MaxMessageBufferBytes)
                    {
                        Log(string.Format(
                            "受信バッファ上限超過（{0} bytes）。クライアントを切断します。",
                            MaxMessageBufferBytes), isError: true);
                        break;
                    }

                    messageBuffer.Append(Encoding.UTF8.GetString(buffer, 0, bytesRead));

                    // CR+LF で分割してメッセージを取り出す
                    string accumulated = messageBuffer.ToString();
                    int idx;
                    while ((idx = accumulated.IndexOf("\r\n", StringComparison.Ordinal)) >= 0)
                    {
                        string line = accumulated.Substring(0, idx);
                        accumulated = accumulated.Substring(idx + 2); // "\r\n" の 2 文字分スキップ

                        if (!string.IsNullOrEmpty(line))
                        {
                            Log(string.Format("受信: {0}", line));

                            // イベントハンドラに返信用コールバックを渡す
                            var handler = MessageReceived;
                            if (handler != null)
                            {
                                try
                                {
                                    await handler.Invoke(line, replySender);
                                }
                                catch (Exception ex)
                                {
                                    Log(string.Format("MessageReceived ハンドラエラー: {0}", ex.Message), isError: true);
                                }
                            }
                        }
                    }

                    messageBuffer.Clear();
                    messageBuffer.Append(accumulated);
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                Log(string.Format("受信エラー: {0}", ex.Message), isError: true);
            }
            finally
            {
                RemoveClient(client);
                Log("クライアント切断");
            }
        }

        // =================================================================
        // ユーティリティ
        // =================================================================

        /// <summary>クライアントをリストから除去してクローズする</summary>
        private void RemoveClient(TcpClient client)
        {
            bool removed;
            lock (_lock) { removed = _connectedClients.Remove(client); }

            if (removed)
            {
                try { client.Close(); } catch { }
                ClientCountChanged?.Invoke(ConnectedClientCount);
            }
        }

        private void Log(string message, bool isError = false)
        {
            LogMessage?.Invoke(string.Format("[ServerProxy] {0}", message), isError);
        }

        // =================================================================
        // IDisposable
        // =================================================================

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            Stop();

            _cts?.Dispose();
            _cts = null;
        }
    }
}
