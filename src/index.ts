import "dotenv/config";
import express from "express";
import { Pool } from "pg";
import * as line from "@line/bot-sdk";
import type { Readable } from "node:stream";

// ======================= config =======================
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

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// ======================= หมวดหมู่ & คำที่ใช้เดาหมวด =======================
const EXPENSE_CATS = ["อาหาร", "เดินทาง", "ช้อปปิ้ง", "ที่พัก", "บิล/ค่าบริการ", "สุขภาพ", "บันเทิง", "การศึกษา", "อื่นๆ"];
const INCOME_CATS = ["เงินเดือน", "เงินจากที่บ้าน", "รายได้เสริม", "อื่นๆ"];
const catsOf = (type: string) => (type === "income" ? INCOME_CATS : EXPENSE_CATS);

// แก้/เพิ่มคำได้ตามใจ: "หมวด": [คำที่เจอในข้อความแล้วให้เดาเป็นหมวดนี้]
const KEYWORDS: Record<"expense" | "income", Record<string, string[]>> = {
  expense: {
    อาหาร: ["ข้าว", "กาแฟ", "ชานม", "ชา", "น้ำ", "ก๋วยเตี๋ยว", "อาหาร", "กิน", "ขนม", "ชาบู", "หมูกระทะ", "ส้มตำ", "เครื่องดื่ม", "โรงอาหาร", "food", "cafe"],
    เดินทาง: ["รถ", "น้ำมัน", "แท็กซี่", "bts", "mrt", "วิน", "grab", "bolt", "ทางด่วน", "ตั๋ว", "เครื่องบิน", "ค่าเดินทาง"],
    ช้อปปิ้ง: ["ซื้อ", "เสื้อ", "รองเท้า", "กระเป๋า", "shopee", "lazada", "ช้อป"],
    ที่พัก: ["ค่าเช่า", "ค่าหอ", "หอพัก", "ค่าห้อง"],
    "บิล/ค่าบริการ": ["ค่าไฟ", "ค่าน้ำ", "ค่าเน็ต", "อินเทอร์เน็ต", "เน็ต", "ค่าโทร", "เติมเงิน", "ค่าบริการ"],
    สุขภาพ: ["ยา", "หมอ", "โรงพยาบาล", "ฟัน", "คลินิก"],
    บันเทิง: ["หนัง", "เกม", "netflix", "spotify", "คอนเสิร์ต", "เที่ยว"],
    การศึกษา: ["หนังสือ", "ค่าเทอม", "คอร์ส", "เรียน", "ค่าลงทะเบียน", "ปริ้น", "เอกสาร"],
  },
  income: {
    เงินเดือน: ["เงินเดือน", "salary"],
    เงินจากที่บ้าน: ["ค่าขนม", "ที่บ้าน", "แม่", "พ่อ", "ผู้ปกครอง"],
    รายได้เสริม: ["ฟรีแลนซ์", "ค่าจ้าง", "งาน", "ขายของ", "ติว"],
  },
};

function guessCategory(type: "expense" | "income", note: string | null): string | null {
  if (!note) return null;
  const n = note.toLowerCase();
  for (const [cat, words] of Object.entries(KEYWORDS[type])) {
    if (words.some((w) => n.includes(w))) return cat;
  }
  return null;
}

// ======================= database =======================
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
  // คอลัมน์ใหม่ (ปลอดภัยต่อฐานข้อมูลเดิม)
  await pool.query(`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS category TEXT`);
  await pool.query(`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS note TEXT`);
  await pool.query(`ALTER TABLE transactions ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'slip'`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS settings (
      line_user TEXT PRIMARY KEY,
      my_name   TEXT,
      budget    NUMERIC(14,2)
    )
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS payee_rules (
      line_user TEXT NOT NULL,
      payee     TEXT NOT NULL,
      type      TEXT NOT NULL,
      category  TEXT NOT NULL,
      PRIMARY KEY (line_user, payee, type)
    )
  `);
  // รายการที่ค้างไม่ได้เลือกเกิน 2 วัน ลบทิ้ง
  await pool.query(`DELETE FROM transactions WHERE status = 'pending' AND created_at < now() - interval '2 days'`);
}

// เงื่อนไขช่วงเวลา (เวลาไทย)
const TS = `COALESCE(tx_datetime, created_at AT TIME ZONE 'Asia/Bangkok')`;
const NOW = `now() AT TIME ZONE 'Asia/Bangkok'`;
const PERIOD = {
  today: { title: "วันนี้", cond: `date_trunc('day', ${TS}) = date_trunc('day', ${NOW})` },
  month: { title: "เดือนนี้", cond: `date_trunc('month', ${TS}) = date_trunc('month', ${NOW})` },
  lastmonth: { title: "เดือนก่อน", cond: `date_trunc('month', ${TS}) = date_trunc('month', ${NOW}) - interval '1 month'` },
} as const;
type PeriodKey = keyof typeof PERIOD;

// ======================= อ่านสลีปด้วย Gemini =======================
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

// ======================= helpers =======================
type Tx = {
  id: number;
  amount: number;
  type: "income" | "expense";
  category: string | null;
  sender: string | null;
  receiver: string | null;
  note: string | null;
  source: string;
};
const TX_COLS = `id, amount::float AS amount, type, category, sender, receiver, note, source`;

const baht = (n: number) => n.toLocaleString("th-TH", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

function normalizeDatetime(s: string | null): string | null {
  if (!s) return null;
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})/);
  if (!m) return null;
  let year = Number(m[1]);
  if (year > 2400) year -= 543;
  return `${year}-${m[2]}-${m[3]} ${m[4]}:${m[5]}:00`;
}

function fmtDt(s: string | null): string {
  const m = s?.match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})/);
  return m ? `${m[3]}/${m[2]}/${Number(m[1]) + 543} ${m[4]}:${m[5]}` : "-";
}

// ทำชื่อให้เทียบกันได้: ตัดคำนำหน้า ช่องว่าง และส่วนที่ถูกปิดด้วย *
function norm(s?: string | null): string {
  return (s ?? "")
    .split("*")[0]
    .trim()
    .replace(/^(นางสาว|นาง|นาย|น\.ส\.|ด\.ช\.|ด\.ญ\.|mr\.?|mrs\.?|ms\.?|miss)\s*/i, "")
    .replace(/\s+/g, "")
    .toLowerCase();
}

function qr(items: { label: string; data: string }[]): line.messagingApi.QuickReply {
  return {
    items: items.map((i) => ({
      type: "action" as const,
      action: { type: "postback" as const, label: i.label, data: i.data, displayText: i.label },
    })),
  };
}

function reply(token: string, text: string, quick?: line.messagingApi.QuickReply) {
  return client.replyMessage({
    replyToken: token,
    messages: [{ type: "text", text, ...(quick ? { quickReply: quick } : {}) }],
  });
}

function catQuick(id: number, type: string, opts: { swap?: boolean; cancel?: boolean } = {}) {
  const items = catsOf(type).map((c, i) => ({ label: c, data: `a=cat&id=${id}&c=${i}` }));
  if (opts.swap) {
    const other = type === "expense" ? "income" : "expense";
    items.push({ label: other === "income" ? "เปลี่ยนเป็นรายรับ" : "เปลี่ยนเป็นรายจ่าย", data: `a=type&id=${id}&t=${other}` });
  }
  if (opts.cancel) items.push({ label: "ยกเลิก", data: `a=cancel&id=${id}` });
  return qr(items);
}

const confirmQuick = (id: number) =>
  qr([
    { label: "เปลี่ยนหมวด", data: `a=recat&id=${id}` },
    { label: "ยกเลิก", data: `a=cancel&id=${id}` },
  ]);

async function budgetLine(userId: string): Promise<string> {
  const s = await pool.query("SELECT budget::float AS budget FROM settings WHERE line_user = $1", [userId]);
  const budget: number | null = s.rows[0]?.budget ?? null;
  if (!budget) return "";
  const r = await pool.query(
    `SELECT COALESCE(SUM(amount),0)::float AS spent FROM transactions
     WHERE line_user = $1 AND status = 'confirmed' AND type = 'expense' AND ${PERIOD.month.cond}`,
    [userId]
  );
  const spent: number = r.rows[0].spent;
  const pct = Math.round((spent / budget) * 100);
  let t = `\n💰 งบเดือนนี้: ใช้ไป ${pct}% (${baht(spent)}/${baht(budget)})`;
  if (pct >= 100) t += "\n⚠️ เกินงบแล้ว";
  else if (pct >= 80) t += "\n⚠️ ใกล้เต็มงบแล้ว";
  return t;
}

async function confirmText(tx: Tx, userId: string, auto: boolean): Promise<string> {
  const kind = tx.type === "income" ? "รายรับ" : "รายจ่าย";
  let t = `✅ บันทึก${auto ? "อัตโนมัติ (จำจากครั้งก่อน)" : "แล้ว"}\n${kind} ${baht(tx.amount)} บาท • ${tx.category}`;
  const who = tx.type === "income" ? tx.sender : tx.receiver;
  if (tx.note) t += `\n📝 ${tx.note}`;
  else if (who) t += `\n${tx.type === "income" ? "จาก" : "ถึง"}: ${who}`;
  if (tx.type === "expense") t += await budgetLine(userId);
  return t;
}

// ยืนยันรายการ + จำผู้รับ/ผู้โอนนี้ไว้ใช้ครั้งหน้า
async function finalize(id: number, userId: string, category: string, type?: string): Promise<Tx | null> {
  const r = await pool.query(
    `UPDATE transactions
     SET category = $1, status = 'confirmed', type = COALESCE($4::text, type)
     WHERE id = $2 AND line_user = $3 AND COALESCE($4::text, type) IS NOT NULL
     RETURNING ${TX_COLS}`,
    [category, id, userId, type ?? null]
  );
  const tx: Tx | undefined = r.rows[0];
  if (!tx) return null;
  if (tx.source === "slip") {
    const key = norm(tx.type === "income" ? tx.sender : tx.receiver);
    if (key) {
      await pool.query(
        `INSERT INTO payee_rules (line_user, payee, type, category) VALUES ($1, $2, $3, $4)
         ON CONFLICT (line_user, payee, type) DO UPDATE SET category = EXCLUDED.category`,
        [userId, key, tx.type, category]
      );
    }
  }
  return tx;
}

async function findRule(userId: string, recvKey: string, sendKey: string) {
  if (!recvKey && !sendKey) return null;
  const r = await pool.query(
    `SELECT type, category FROM payee_rules
     WHERE line_user = $1 AND ((payee = $2 AND type = 'expense') OR (payee = $3 AND type = 'income'))
     LIMIT 1`,
    [userId, recvKey || "__none__", sendKey || "__none__"]
  );
  return (r.rows[0] as { type: string; category: string } | undefined) ?? null;
}

// ======================= รับรูปสลีป =======================
async function onImage(event: line.MessageEvent, userId: string) {
  const token = event.replyToken!;
  const messageId = (event.message as unknown as { id: string }).id;
  let slip: Slip;
  try {
    slip = await readSlip(messageId);
  } catch (err) {
    console.error(err);
    return reply(token, "อ่านรูปนี้ไม่ได้ ลองส่งสลีปที่ชัดขึ้นอีกครั้งนะครับ");
  }
  const amount = Number(slip.amount);
  if (!amount || amount <= 0) return reply(token, "ไม่พบยอดเงินในรูป รูปนี้อาจไม่ใช่สลีปครับ");

  const ref = slip.ref?.trim() || null;
  if (ref) {
    const dup = await pool.query("SELECT id, status FROM transactions WHERE slip_ref = $1", [ref]);
    if (dup.rows[0]) {
      if (dup.rows[0].status === "confirmed") return reply(token, "สลีปนี้เคยบันทึกไปแล้วครับ");
      await pool.query("DELETE FROM transactions WHERE id = $1 AND line_user = $2", [dup.rows[0].id, userId]);
    }
  }

  const dt = normalizeDatetime(slip.datetime);
  let id: number;
  try {
    const r = await pool.query(
      `INSERT INTO transactions (line_user, amount, tx_datetime, sender, receiver, bank, slip_ref, source)
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'slip') RETURNING id`,
      [userId, amount, dt, slip.sender, slip.receiver, slip.bank, ref]
    );
    id = r.rows[0].id;
  } catch (err: any) {
    if (err.code === "23505") return reply(token, "สลีปนี้เคยบันทึกไปแล้วครับ");
    throw err;
  }

  const card = `พบสลีป ${baht(amount)} บาท\nจาก: ${slip.sender ?? "-"}\nถึง: ${slip.receiver ?? "-"}\nเวลา: ${fmtDt(dt)}`;

  // 1) เคยบันทึกผู้รับ/ผู้โอนนี้แล้ว -> บันทึกให้เลย
  const rule = await findRule(userId, norm(slip.receiver), norm(slip.sender));
  if (rule) {
    const tx = await finalize(id, userId, rule.category, rule.type);
    if (tx) return reply(token, `${card}\n\n${await confirmText(tx, userId, true)}`, confirmQuick(id));
  }

  // 2) เดารายรับ/รายจ่ายจากชื่อของฉัน
  const s = await pool.query("SELECT my_name FROM settings WHERE line_user = $1", [userId]);
  const me: string = s.rows[0]?.my_name ?? "";
  if (me.length >= 2) {
    const fromMe = norm(slip.sender).includes(me);
    const toMe = norm(slip.receiver).includes(me);
    const guess = fromMe && !toMe ? "expense" : toMe && !fromMe ? "income" : null;
    if (guess) {
      await pool.query("UPDATE transactions SET type = $1 WHERE id = $2", [guess, id]);
      const kind = guess === "income" ? "รายรับ" : "รายจ่าย";
      return reply(token, `${card}\n\nเป็น${kind} เลือกหมวดได้เลย`, catQuick(id, guess, { swap: true, cancel: true }));
    }
  }

  // 3) ถามเอง
  return reply(
    token,
    `${card}\n\nรายการนี้เป็น?`,
    qr([
      { label: "รายรับ", data: `a=type&id=${id}&t=income` },
      { label: "รายจ่าย", data: `a=type&id=${id}&t=expense` },
      { label: "ยกเลิก", data: `a=cancel&id=${id}` },
    ])
  );
}

// ======================= กดปุ่ม =======================
async function onPostback(event: line.PostbackEvent, userId: string) {
  const token = event.replyToken!;
  const p = new URLSearchParams(event.postback.data);
  const a = p.get("a");
  const id = Number(p.get("id"));

  if (a === "cancel") {
    await pool.query("DELETE FROM transactions WHERE id = $1 AND line_user = $2", [id, userId]);
    return reply(token, "ลบรายการแล้วครับ");
  }

  if (a === "type") {
    const t = p.get("t");
    if (t !== "income" && t !== "expense") return;
    const r = await pool.query(
      "UPDATE transactions SET type = $1 WHERE id = $2 AND line_user = $3 AND status = 'pending' RETURNING id",
      [t, id, userId]
    );
    if (!r.rowCount) return reply(token, "ไม่พบรายการนี้แล้วครับ");
    return reply(token, `เลือกหมวด${t === "income" ? "รายรับ" : "รายจ่าย"}`, catQuick(id, t, { cancel: true }));
  }

  if (a === "recat" || a === "cat") {
    const r = await pool.query("SELECT type FROM transactions WHERE id = $1 AND line_user = $2", [id, userId]);
    const type: string | undefined = r.rows[0]?.type;
    if (!type) return reply(token, "ไม่พบรายการนี้แล้วครับ");
    if (a === "recat") return reply(token, "เปลี่ยนเป็นหมวดอะไร?", catQuick(id, type));
    const category = catsOf(type)[Number(p.get("c"))];
    if (!category) return;
    const tx = await finalize(id, userId, category);
    if (!tx) return reply(token, "ไม่พบรายการนี้แล้วครับ");
    return reply(token, await confirmText(tx, userId, false), confirmQuick(id));
  }
}

// ======================= พิมพ์ข้อความ =======================
const HELP = `วิธีใช้
📷 ส่งรูปสลีป → เลือกรายรับ/รายจ่าย และหมวด (ครั้งต่อไปถ้าผู้รับคนเดิม บอทจะบันทึกให้เอง)
✍️ พิมพ์บันทึกเอง เช่น
   จ่าย 120 ข้าวมันไก่
   รับ 500 ค่าขนม
📊 สรุป / สรุปวันนี้ / สรุปเดือนก่อน
📋 รายการ  (ดู 10 รายการล่าสุด)
🗑 ลบล่าสุด
👤 ชื่อฉัน ธาลินี  (บอทจะเดารายรับ/รายจ่ายจากชื่อ)
💰 ตั้งงบ 8000  (แจ้งเตือนเมื่อใกล้เต็มงบ, ตั้งงบ 0 = ปิด)`;

async function summary(userId: string, period: PeriodKey): Promise<string> {
  const { title, cond } = PERIOD[period];
  const r = await pool.query(
    `SELECT type, COALESCE(category, 'ไม่ระบุ') AS category, SUM(amount)::float AS total, COUNT(*)::int AS n
     FROM transactions
     WHERE line_user = $1 AND status = 'confirmed' AND ${cond}
     GROUP BY type, category ORDER BY total DESC`,
    [userId]
  );
  const rows = r.rows as { type: string; category: string; total: number; n: number }[];
  if (!rows.length) return `ยังไม่มีรายการ${title}ครับ`;
  const sum = (t: string) => rows.filter((x) => x.type === t).reduce((a, x) => a + x.total, 0);
  const cnt = (t: string) => rows.filter((x) => x.type === t).reduce((a, x) => a + x.n, 0);
  const inc = sum("income");
  const exp = sum("expense");
  let t = `สรุป${title}\nรายรับ: ${baht(inc)} บาท (${cnt("income")} รายการ)\nรายจ่าย: ${baht(exp)} บาท (${cnt("expense")} รายการ)\nคงเหลือ: ${baht(inc - exp)} บาท`;
  const byCat = rows.filter((x) => x.type === "expense");
  if (byCat.length) {
    t += "\n\nรายจ่ายแยกหมวด";
    for (const c of byCat) t += `\n• ${c.category} ${baht(c.total)} (${Math.round((c.total / exp) * 100)}%)`;
  }
  if (period === "month") t += await budgetLine(userId);
  return t;
}

async function onText(event: line.MessageEvent, userId: string) {
  const token = event.replyToken!;
  const text = (event.message as unknown as { text: string }).text.trim().replace(/\s+/g, " ");

  if (/^(ช่วยเหลือ|เมนู|help)$/i.test(text)) return reply(token, HELP);

  if (text === "สรุป") return reply(token, await summary(userId, "month"));
  if (text === "สรุปวันนี้") return reply(token, await summary(userId, "today"));
  if (text === "สรุปเดือนก่อน") return reply(token, await summary(userId, "lastmonth"));

  if (text === "รายการ") {
    const r = await pool.query(
      `SELECT type, amount::float AS amount, COALESCE(category, '-') AS category,
              COALESCE(note, CASE WHEN type = 'income' THEN sender ELSE receiver END, '') AS label,
              to_char(${TS}, 'DD/MM HH24:MI') AS d
       FROM transactions WHERE line_user = $1 AND status = 'confirmed'
       ORDER BY id DESC LIMIT 10`,
      [userId]
    );
    if (!r.rows.length) return reply(token, "ยังไม่มีรายการครับ");
    const lines = r.rows.map(
      (x: any) => `${x.d} ${x.type === "income" ? "+" : "-"}${baht(x.amount)} ${x.category} ${x.label}`.trim()
    );
    return reply(token, `10 รายการล่าสุด\n${lines.join("\n")}\n\nพิมพ์ "ลบล่าสุด" ถ้าต้องการลบอันบนสุด`);
  }

  if (text === "ลบล่าสุด") {
    const r = await pool.query(
      `DELETE FROM transactions WHERE id = (
         SELECT id FROM transactions WHERE line_user = $1 AND status = 'confirmed' ORDER BY id DESC LIMIT 1
       ) RETURNING type, amount::float AS amount, category`,
      [userId]
    );
    const x = r.rows[0];
    if (!x) return reply(token, "ไม่มีรายการให้ลบครับ");
    return reply(token, `ลบแล้ว: ${x.type === "income" ? "รายรับ" : "รายจ่าย"} ${baht(x.amount)} บาท • ${x.category ?? "-"}`);
  }

  let m = text.match(/^ชื่อฉัน (.+)$/);
  if (m) {
    const name = norm(m[1]);
    if (name.length < 2) return reply(token, "ชื่อสั้นเกินไปครับ ลองพิมพ์ชื่อจริงอย่างน้อย 2 ตัวอักษร");
    await pool.query(
      `INSERT INTO settings (line_user, my_name) VALUES ($1, $2)
       ON CONFLICT (line_user) DO UPDATE SET my_name = EXCLUDED.my_name`,
      [userId, name]
    );
    return reply(token, `จำชื่อแล้ว ต่อไปสลีปที่โอนจากชื่อนี้จะเดาเป็นรายจ่าย และโอนเข้าชื่อนี้จะเดาเป็นรายรับครับ`);
  }

  m = text.match(/^ตั้งงบ ([\d,]+(?:\.\d+)?)$/);
  if (m) {
    const v = Number(m[1].replace(/,/g, ""));
    await pool.query(
      `INSERT INTO settings (line_user, budget) VALUES ($1, $2)
       ON CONFLICT (line_user) DO UPDATE SET budget = EXCLUDED.budget`,
      [userId, v > 0 ? v : null]
    );
    return reply(token, v > 0 ? `ตั้งงบรายจ่ายรายเดือน ${baht(v)} บาทแล้วครับ` : "ปิดการแจ้งเตือนงบแล้วครับ");
  }

  // พิมพ์บันทึกเอง: "จ่าย 120 ข้าวมันไก่" / "รับ 500 ค่าขนม"
  m = text.match(/^(จ่าย|รับ) ?([\d,]+(?:\.\d{1,2})?) ?(.*)$/);
  if (m) {
    const type = m[1] === "จ่าย" ? "expense" : "income";
    const amount = Number(m[2].replace(/,/g, ""));
    const note = m[3].trim() || null;
    if (!amount) return reply(token, "ยอดเงินไม่ถูกต้องครับ");
    const r = await pool.query(
      `INSERT INTO transactions (line_user, amount, type, note, source) VALUES ($1, $2, $3, $4, 'text') RETURNING id`,
      [userId, amount, type, note]
    );
    const id: number = r.rows[0].id;
    const cat = guessCategory(type, note);
    if (cat) {
      const tx = await finalize(id, userId, cat);
      if (tx) return reply(token, await confirmText(tx, userId, false), confirmQuick(id));
    }
    return reply(token, `${type === "income" ? "รายรับ" : "รายจ่าย"} ${baht(amount)} บาท${note ? ` (${note})` : ""}\nเลือกหมวด`, catQuick(id, type, { cancel: true }));
  }

  return reply(token, `ส่งรูปสลีป หรือพิมพ์ เช่น "จ่าย 120 ข้าว" ได้เลยครับ\nพิมพ์ "ช่วยเหลือ" เพื่อดูคำสั่งทั้งหมด`);
}

async function handleEvent(event: line.WebhookEvent) {
  const userId = event.source?.userId;
  if (!userId) return;
  if (event.type === "message" && event.message.type === "image") return onImage(event, userId);
  if (event.type === "message" && event.message.type === "text") return onText(event, userId);
  if (event.type === "postback") return onPostback(event, userId);
}

// ======================= server =======================
async function main() {
  await initDb();

  const app = express();
  app.post("/webhook", line.middleware(lineConfig), (req, res) => {
    res.sendStatus(200); // ตอบ LINE ทันที แล้วค่อยประมวลผลต่อ
    const events: line.WebhookEvent[] = req.body.events;
    for (const e of events) handleEvent(e).catch((err) => console.error(err));
  });
  app.get("/health", (_req, res) => res.send("ok"));
  app.get("/", (_req, res) => res.send("LINE slip bot is running"));

  const port = Number(process.env.PORT) || 3000;
  app.listen(port, () => console.log(`Listening on :${port}`));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
