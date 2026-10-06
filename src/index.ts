import "dotenv/config";
import express from "express";
import { Pool } from "pg";
import * as line from "@line/bot-sdk";
import type { Readable } from "node:stream";

// ---------- config ----------
const lineConfig = {
  channelAccessToken: process.env.LINE_CHANNEL_ACCESS_TOKEN!,
  channelSecret: process.env.LINE_CHANNEL_SECRET!,
};
const GEMINI_KEY = process.env.GEMINI_API_KEY!;
const MODEL = process.env.GEMINI_MODEL || "gemini-3.8-flash";

const client = new line.messagingApi.MessagingApiClient({
  channelAccessToken: lineConfig.channelAccessToken,
});
const blob = new line.messagingApi.MessagingApiBlobClient({
  channelAccessToken: lineConfig.channelAccessToken,
});

// ---------- database (Postgres เช่น Neon / Supabase) ----------
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS transactions (
      id          SERIAL PRIMARY KEY,
      line_user   TEXT NOT NULL,
      amount      NUMERIC(14,2) NOT NULL,
      type        TEXT CHECK (type IN ('income','expense')),
      status      TEXT NOT NULL DEFAULT 'pending',
      tx_datetime TIMESTAMP,
      sender      TEXT,
      receiver    TEXT,
      bank        TEXT,
      slip_ref    TEXT UNIQUE,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
}

// ---------- อ่านสลีปด้วย Gemini (มีโควตาฟรี) ----------
type Slip = {
  amount: number | null;
  datetime: string | null;
  sender: string | null;
  receiver: string | null;
  bank: string | null;
  ref: string | null;
};

const PROMPT = `นี่คือสลีปโอนเงินของธนาคารไทย ดึงข้อมูลแล้วตอบเป็น JSON เท่านั้น ห้ามมีข้อความอื่นหรือ markdown
รูปแบบ: {"amount": number, "datetime": "YYYY-MM-DDTHH:mm" (ค.ศ.), "sender": string, "receiver": string, "bank": string, "ref": string}
ถ้าไม่ใช่สลีปหรืออ่านค่าไหนไม่ได้ ให้ใส่ null`;

async function streamToBuffer(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const c of stream) chunks.push(Buffer.from(c));
  return Buffer.concat(chunks);
}

async function readSlip(messageId: string): Promise<Slip> {
  const img = await streamToBuffer(await blob.getMessageContent(messageId));
  const body = JSON.stringify({
    contents: [
      {
        parts: [
          { inline_data: { mime_type: "image/jpeg", data: img.toString("base64") } },
          { text: PROMPT },
        ],
      },
    ],
    generationConfig: { responseMimeType: "application/json" },
  });

  // ลองโมเดลหลักก่อน ถ้าเต็ม (503) หรือเกินโควตา (429) จะรอแล้วลองใหม่ และสลับไปโมเดลสำรอง
  const models = [MODEL, "gemini-flash-latest"];
  let res: Response | undefined;
  for (const m of models) {
    for (let attempt = 0; attempt < 3; attempt++) {
      res = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/${m}:generateContent`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json", "x-goog-api-key": GEMINI_KEY },
          body,
        }
      );
      if (res.ok || (res.status !== 503 && res.status !== 429)) break;
      await new Promise((r) => setTimeout(r, 1500 * (attempt + 1)));
    }
    if (res?.ok) break;
  }
  if (!res || !res.ok) throw new Error(`Gemini error ${res?.status}: ${await res?.text()}`);
  const data: any = await res.json();
  const text: string = data.candidates?.[0]?.content?.parts?.[0]?.text ?? "";
  const json = text.match(/\{[\s\S]*\}/)?.[0];
  if (!json) throw new Error("อ่านสลีปไม่ได้");
  return JSON.parse(json) as Slip;
}

// แปลงวันเวลาจากสลีปให้ปลอดภัย (กันรูปแบบผิด และกันปี พ.ศ.)
function normalizeDatetime(s: string | null): string | null {
  if (!s) return null;
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})/);
  if (!m) return null;
  let year = Number(m[1]);
  if (year > 2400) year -= 543;
  return `${year}-${m[2]}-${m[3]} ${m[4]}:${m[5]}:00`;
}

// ---------- handlers ----------
const baht = (n: number) => n.toLocaleString("th-TH", { minimumFractionDigits: 2 });

function reply(replyToken: string, text: string) {
  return client.replyMessage({ replyToken, messages: [{ type: "text", text }] });
}

async function onImage(event: line.MessageEvent, userId: string) {
  const msg = event.message as line.ImageMessage;
  let slip: Slip;
  try {
    slip = await readSlip(msg.id);
  } catch (err) {
    console.error(err);
    return reply(event.replyToken!, "อ่านรูปนี้ไม่ได้ ลองส่งสลีปที่ชัดขึ้นอีกครั้งนะครับ");
  }
  const amount = Number(slip.amount);
  if (!amount || amount <= 0) {
    return reply(event.replyToken!, "ไม่พบยอดเงินในรูป รูปนี้อาจไม่ใช่สลีปครับ");
  }

  if (slip.ref) {
    const dup = await pool.query("SELECT id FROM transactions WHERE slip_ref = $1", [slip.ref]);
    if (dup.rowCount) return reply(event.replyToken!, "สลีปนี้เคยบันทึกไปแล้วครับ");
  }

  const dt = normalizeDatetime(slip.datetime);
  let id: number;
  try {
    const r = await pool.query(
      `INSERT INTO transactions (line_user, amount, tx_datetime, sender, receiver, bank, slip_ref)
       VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id`,
      [userId, amount, dt, slip.sender, slip.receiver, slip.bank, slip.ref]
    );
    id = r.rows[0].id;
  } catch (err: any) {
    if (err.code === "23505") return reply(event.replyToken!, "สลีปนี้เคยบันทึกไปแล้วครับ");
    throw err;
  }

  const text =
    `พบสลีป ${baht(amount)} บาท\n` +
    `จาก: ${slip.sender ?? "-"}\nถึง: ${slip.receiver ?? "-"}\n` +
    `เวลา: ${dt ?? "-"}\n\nรายการนี้เป็น?`;

  return client.replyMessage({
    replyToken: event.replyToken!,
    messages: [
      {
        type: "text",
        text,
        quickReply: {
          items: [
            { type: "action", action: { type: "postback", label: "รายรับ", data: `id=${id}&t=income`, displayText: "รายรับ" } },
            { type: "action", action: { type: "postback", label: "รายจ่าย", data: `id=${id}&t=expense`, displayText: "รายจ่าย" } },
            { type: "action", action: { type: "postback", label: "ยกเลิก", data: `id=${id}&t=cancel`, displayText: "ยกเลิก" } },
          ],
        },
      },
    ],
  });
}

async function onPostback(event: line.PostbackEvent, userId: string) {
  const p = new URLSearchParams(event.postback.data);
  const id = Number(p.get("id"));
  const t = p.get("t");

  if (t === "cancel") {
    await pool.query(
      "DELETE FROM transactions WHERE id = $1 AND line_user = $2 AND status = 'pending'",
      [id, userId]
    );
    return reply(event.replyToken!, "ยกเลิกรายการแล้วครับ");
  }
  if (t !== "income" && t !== "expense") return;

  const r = await pool.query(
    "UPDATE transactions SET type = $1, status = 'confirmed' WHERE id = $2 AND line_user = $3 AND status = 'pending'",
    [t, id, userId]
  );
  if (!r.rowCount) return reply(event.replyToken!, "รายการนี้ถูกบันทึกหรือยกเลิกไปแล้วครับ");
  return reply(event.replyToken!, `บันทึกเป็น${t === "income" ? "รายรับ" : "รายจ่าย"}เรียบร้อย ✅`);
}

async function onText(event: line.MessageEvent, userId: string) {
  const text = (event.message as line.TextMessage).text.trim();
  if (!text.startsWith("สรุป")) {
    return reply(event.replyToken!, "ส่งรูปสลีปมาได้เลยครับ หรือพิมพ์ \"สรุป\" เพื่อดูยอดเดือนนี้");
  }
  const r = await pool.query(
    `SELECT type, SUM(amount)::float AS total, COUNT(*)::int AS n
     FROM transactions
     WHERE line_user = $1 AND status = 'confirmed'
       AND date_trunc('month', COALESCE(tx_datetime, created_at AT TIME ZONE 'Asia/Bangkok'))
         = date_trunc('month', now() AT TIME ZONE 'Asia/Bangkok')
     GROUP BY type`,
    [userId]
  );
  const rows = r.rows as { type: string; total: number; n: number }[];
  const income = rows.find((x) => x.type === "income");
  const expense = rows.find((x) => x.type === "expense");
  const i = income?.total ?? 0;
  const e = expense?.total ?? 0;
  return reply(
    event.replyToken!,
    `สรุปเดือนนี้\nรายรับ: ${baht(i)} บาท (${income?.n ?? 0} รายการ)\nรายจ่าย: ${baht(e)} บาท (${expense?.n ?? 0} รายการ)\nคงเหลือ: ${baht(i - e)} บาท`
  );
}

async function handleEvent(event: line.WebhookEvent) {
  const userId = event.source?.userId;
  if (!userId) return;
  if (event.type === "message" && event.message.type === "image") return onImage(event, userId);
  if (event.type === "message" && event.message.type === "text") return onText(event, userId);
  if (event.type === "postback") return onPostback(event, userId);
}

// ---------- server ----------
async function main() {
  await initDb();

  const app = express();
  app.post("/webhook", line.middleware(lineConfig), (req, res) => {
    res.sendStatus(200); // ตอบ LINE ทันที แล้วค่อยประมวลผลต่อ
    const events: line.WebhookEvent[] = req.body.events;
    for (const e of events) handleEvent(e).catch((err) => console.error(err));
  });
  // ลิงก์สำหรับ cron job เรียกเพื่อไม่ให้เซิร์ฟเวอร์หลับ
  app.get("/health", (_req, res) => res.send("ok"));
  app.get("/", (_req, res) => res.send("LINE slip bot is running"));

  const port = Number(process.env.PORT) || 3000;
  app.listen(port, () => console.log(`Listening on :${port}`));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
