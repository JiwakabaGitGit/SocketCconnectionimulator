// =========================================================================
// ServerProxy 使用サンプル (.NET Framework 4.8)
//
// 【サンプル内容】
//   Sample1_EchoServer          … 受信メッセージをそのまま送信元へ返すエコーサーバ
//   Sample2_CommandServer       … コマンド解析して返信するサーバ
//   Sample3_BroadcastServer     … 受信メッセージを全クライアントへブロードキャスト
//   Sample4_ManualSend          … 外部からブロードキャスト送信（コンソール入力）
//
// 【動作確認方法】
//   Windowsの場合: telnet 127.0.0.1 9000
//   PowerShellの場合:
//     $tcp = New-Object System.Net.Sockets.TcpClient("127.0.0.1", 9000)
//     $stream = $tcp.GetStream()
//     $writer = New-Object System.IO.StreamWriter($stream)
//     $writer.AutoFlush = $true
//     $writer.WriteLine("Hello")
// =========================================================================
using System;
using System.Threading;
using System.Threading.Tasks;
using SocketApp.Models;

namespace SocketApp.Samples
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("実行するサンプルを選択してください:");
            Console.WriteLine("  1: エコーサーバ（受信メッセージをそのまま返信）");
            Console.WriteLine("  2: コマンドサーバ（コマンド解析して返信）");
            Console.WriteLine("  3: ブロードキャストサーバ（全クライアントへ転送）");
            Console.WriteLine("  4: 手動送信サーバ（コンソールから全クライアントへ送信）");
            Console.Write("番号を入力 > ");

            switch (Console.ReadLine()?.Trim())
            {
                case "1": await Sample1_EchoServer(); break;
                case "2": await Sample2_CommandServer(); break;
                case "3": await Sample3_BroadcastServer(); break;
                case "4": await Sample4_ManualSend(); break;
                default:
                    Console.WriteLine("無効な選択です。");
                    break;
            }
        }

        // =================================================================
        // Sample 1: エコーサーバ
        // 受信したメッセージをそのまま送信元クライアントへ返す。
        // =================================================================
        static async Task Sample1_EchoServer()
        {
            const int port = 9000;
            using (var server = new ServerProxy(port))
            {
                // ログをコンソールへ出力
                server.LogMessage += (msg, isError) =>
                {
                    var prev = Console.ForegroundColor;
                    Console.ForegroundColor = isError ? ConsoleColor.Red : ConsoleColor.Gray;
                    Console.WriteLine(msg);
                    Console.ForegroundColor = prev;
                };

                // 接続数の変化を通知
                server.ClientCountChanged += count =>
                    Console.WriteLine(string.Format("[Info] 接続クライアント数: {0}", count));

                // 受信 → そのまま返信（エコー）
                server.MessageReceived += async (message, reply) =>
                {
                    string response = string.Format("ECHO: {0}", message);
                    await reply(response);
                };

                server.Start();
                Console.WriteLine(string.Format("エコーサーバ起動中（ポート {0}）。Enter で停止します。", port));
                Console.ReadLine();
            } // Dispose → Stop が呼ばれる
        }

        // =================================================================
        // Sample 2: コマンドサーバ
        // 受信メッセージをコマンドとして解析し、対応する返信を行う。
        //
        // 対応コマンド:
        //   PING          → PONG
        //   HELLO <名前>  → HELLO, <名前>!
        //   TIME          → 現在時刻
        //   QUIT          → BYE（サーバ側から切断はしないが返信する）
        //   その他        → UNKNOWN COMMAND
        // =================================================================
        static async Task Sample2_CommandServer()
        {
            const int port = 9001;
            using (var server = new ServerProxy(port))
            {
                server.LogMessage += (msg, isError) =>
                {
                    var prev = Console.ForegroundColor;
                    Console.ForegroundColor = isError ? ConsoleColor.Red : ConsoleColor.Gray;
                    Console.WriteLine(msg);
                    Console.ForegroundColor = prev;
                };

                server.ClientCountChanged += count =>
                    Console.WriteLine(string.Format("[Info] 接続クライアント数: {0}", count));

                server.MessageReceived += OnCommandMessageReceived;

                server.Start();
                Console.WriteLine(string.Format(
                    "コマンドサーバ起動中（ポート {0}）。Enter で停止します。", port));
                Console.WriteLine("コマンド例: PING / HELLO World / TIME / QUIT");
                Console.ReadLine();
            }
        }

        // =================================================================
        // Sample 3: ブロードキャストサーバ
        // あるクライアントからの受信メッセージを全クライアントへ転送する。
        // チャットサーバのような動作。
        // =================================================================
        static async Task Sample3_BroadcastServer()
        {
            const int port = 9002;
            using (var server = new ServerProxy(port))
            {
                server.LogMessage += (msg, isError) =>
                {
                    var prev = Console.ForegroundColor;
                    Console.ForegroundColor = isError ? ConsoleColor.Red : ConsoleColor.Gray;
                    Console.WriteLine(msg);
                    Console.ForegroundColor = prev;
                };

                server.ClientCountChanged += count =>
                    Console.WriteLine(string.Format("[Info] 接続クライアント数: {0}", count));

                // 受信 → 全クライアントへブロードキャスト（送信元含む）
                server.MessageReceived += async (message, reply) =>
                {
                    string broadcast = string.Format("[BROADCAST] {0}", message);
                    await server.SendToAllAsync(broadcast);
                };

                server.Start();
                Console.WriteLine(string.Format(
                    "ブロードキャストサーバ起動中（ポート {0}）。Enter で停止します。", port));
                Console.ReadLine();
            }
        }

        // =================================================================
        // コマンド処理メソッド（Sample2 用）
        // MessageReceived イベントハンドラとして登録する。
        // 受信メッセージをコマンドとして解析し、送信元クライアントへ返信する。
        // =================================================================

        /// <summary>
        /// 受信メッセージをコマンドとして解析し、送信元へ返信する。
        /// </summary>
        /// <param name="message">受信メッセージ</param>
        /// <param name="reply">送信元への返信デリゲート</param>
        private static async Task OnCommandMessageReceived(string message, Func<string, Task> reply)
        {
            string trimmed = message.Trim();
            string upper = trimmed.ToUpperInvariant();
            string response;

            if (upper == "PING")
            {
                response = "PONG";
            }
            else if (upper.StartsWith("HELLO "))
            {
                string name = trimmed.Substring(6).Trim();
                response = string.Format("HELLO, {0}!", name);
            }
            else if (upper == "TIME")
            {
                response = string.Format("TIME: {0:yyyy-MM-dd HH:mm:ss}", DateTime.Now);
            }
            else if (upper == "QUIT")
            {
                response = "BYE";
            }
            else
            {
                response = string.Format("UNKNOWN COMMAND: {0}", trimmed);
            }

            await reply(response);
        }

        // =================================================================
        // Sample 4: 手動送信サーバ
        // サーバ側コンソールから入力したテキストを全クライアントへ送信する。
        // クライアントからの受信はログに表示するのみ（返信なし）。
        // =================================================================
        static async Task Sample4_ManualSend()
        {
            const int port = 9003;
            using (var server = new ServerProxy(port))
            {
                server.LogMessage += (msg, isError) =>
                {
                    var prev = Console.ForegroundColor;
                    Console.ForegroundColor = isError ? ConsoleColor.Red : ConsoleColor.Gray;
                    Console.WriteLine(msg);
                    Console.ForegroundColor = prev;
                };

                server.ClientCountChanged += count =>
                    Console.WriteLine(string.Format("[Info] 接続クライアント数: {0}", count));

                // 受信はコンソール表示のみ（ログで表示済みのため処理不要）
                server.MessageReceived += (message, reply) => Task.CompletedTask;

                server.Start();
                Console.WriteLine(string.Format(
                    "手動送信サーバ起動中（ポート {0}）。", port));
                Console.WriteLine("送信するメッセージを入力してください。空 Enter で停止します。");

                while (true)
                {
                    Console.Write("> ");
                    string input = Console.ReadLine();

                    // 空行で終了
                    if (string.IsNullOrEmpty(input)) break;

                    if (server.ConnectedClientCount == 0)
                    {
                        Console.WriteLine("[警告] 接続中のクライアントがいません。");
                        continue;
                    }

                    await server.SendToAllAsync(input);
                }
            }
        }
    }
}
