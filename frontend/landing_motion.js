// =========================================================
// AutoTele Landing Page v4.0 — 3D Pricing + Video BG + Bigger Logo
// =========================================================

const { useState, useEffect, useRef } = React;

const MotionObj = window.Motion || window.framerMotion || {};
const motion = MotionObj.motion || {
  div: (p) => <div {...p} />, h1: (p) => <h1 {...p} />,
  h2: (p) => <h2 {...p} />, p: (p) => <p {...p} />,
  button: (p) => <button {...p} />, span: (p) => <span {...p} />,
  section: (p) => <section {...p} />,
};
const AnimatePresence = MotionObj.AnimatePresence || (({ children }) => children);

// ── Particles ─────────────────────────────────────────
function ParticleBg() {
  const pts = Array.from({ length: 20 }, (_, i) => ({
    id: i, x: Math.random() * 100, y: Math.random() * 100,
    size: Math.random() * 3 + 1, dur: Math.random() * 9 + 6, delay: Math.random() * 5,
  }));
  return (
    <div style={{ position: 'absolute', inset: 0, overflow: 'hidden', pointerEvents: 'none', zIndex: 0 }}>
      {pts.map(p => (
        <motion.div key={p.id}
          style={{ position: 'absolute', left: `${p.x}%`, top: `${p.y}%`, width: p.size, height: p.size, borderRadius: '50%', background: 'rgba(0,180,255,0.55)', boxShadow: '0 0 6px rgba(0,180,255,0.5)' }}
          animate={{ y: [0, -28, 0], opacity: [0.15, 0.7, 0.15] }}
          transition={{ duration: p.dur, delay: p.delay, repeat: Infinity, ease: 'easeInOut' }}
        />
      ))}
    </div>
  );
}

// ── Hero Graphic ──────────────────────────────────────
function HeroGraphic() {
  return (
    <div style={{ position: 'relative', width: '100%', maxWidth: 440, margin: '0 auto' }}>
      <motion.div animate={{ scale: [1, 1.07, 1], opacity: [0.22, 0.52, 0.22] }} transition={{ duration: 4, repeat: Infinity }}
        style={{ position: 'absolute', inset: -40, borderRadius: '50%', background: 'radial-gradient(circle, rgba(0,90,255,0.22) 0%, transparent 70%)', pointerEvents: 'none' }} />
      <motion.div animate={{ y: [-8, 8, -8] }} transition={{ duration: 5, repeat: Infinity, ease: 'easeInOut' }}
        style={{ background: 'linear-gradient(135deg,#091840,#0d2680,#091840)', border: '1.5px solid rgba(0,140,255,0.45)', borderRadius: 24, padding: '36px 28px', boxShadow: '0 0 60px rgba(0,100,255,0.22),inset 0 0 40px rgba(0,80,255,0.06)', position: 'relative', overflow: 'hidden' }}>
        <div style={{ position: 'absolute', top: 0, left: 0, right: 0, height: 1, background: 'linear-gradient(90deg,transparent,rgba(0,180,255,0.65),transparent)' }} />
        <div style={{ display: 'flex', justifyContent: 'center', marginBottom: 24 }}>
          <motion.div animate={{ boxShadow: ['0 0 20px rgba(0,150,255,0.4)', '0 0 55px rgba(0,150,255,0.9)', '0 0 20px rgba(0,150,255,0.4)'] }} transition={{ duration: 2.5, repeat: Infinity }}
            style={{ width: 90, height: 90, borderRadius: '50%', background: 'radial-gradient(circle,rgba(0,80,220,0.9) 0%,rgba(0,22,85,0.9) 100%)', border: '2px solid rgba(0,180,255,0.65)', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <img src="logo.png" alt="AutoTele" style={{ width: 56, height: 56, objectFit: 'contain', filter: 'drop-shadow(0 0 14px rgba(0,230,255,1)) brightness(1.5)' }} />
          </motion.div>
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 9 }}>
          {[
            { label: 'SYSTEM', value: 'CONNECTED', color: '#00ff88' },
            { label: 'CHANNELS', value: '+1,200 ACTIVE', color: '#00c8ff' },
            { label: 'WAVE ENGINE', value: 'RUNNING 24/7', color: '#00c8ff' },
            { label: 'ADS POSTED', value: '+5,000,000', color: '#a78bfa' },
            { label: 'UPTIME', value: '99.9%', color: '#fbbf24' },
          ].map((r, i) => (
            <motion.div key={r.label} initial={{ opacity: 0, x: -16 }} animate={{ opacity: 1, x: 0 }} transition={{ delay: i * 0.1 }}
              style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', background: 'rgba(0,60,180,0.15)', border: '1px solid rgba(0,140,255,0.14)', borderRadius: 8, padding: '7px 13px' }}>
              <span style={{ fontFamily: 'monospace', fontSize: 9, color: 'rgba(180,210,255,0.65)', letterSpacing: 2 }}>{r.label}</span>
              <span style={{ fontFamily: 'monospace', fontSize: 11, fontWeight: 700, color: r.color, letterSpacing: 1 }}>{r.value}</span>
            </motion.div>
          ))}
        </div>
      </motion.div>
      <motion.div animate={{ y: [-5, 5, -5] }} transition={{ duration: 3, repeat: Infinity, delay: 0.5 }}
        style={{ position: 'absolute', top: -18, right: -18, background: 'linear-gradient(135deg,#1a2e80,#0d1f5c)', border: '1px solid rgba(0,180,255,0.42)', borderRadius: 14, padding: '9px 14px', boxShadow: '0 0 20px rgba(0,120,255,0.32)', fontSize: 11, color: '#fff', fontWeight: 700, display: 'flex', alignItems: 'center', gap: 7, whiteSpace: 'nowrap' }}>
        <span style={{ color: '#00ff88', fontSize: 14 }}>●</span>
        <div><div style={{ fontSize: 8, color: 'rgba(180,210,255,0.6)', marginBottom: 1 }}>BOT STATUS</div><div>LIVE & ACTIVE</div></div>
      </motion.div>
      <motion.div animate={{ y: [5, -5, 5] }} transition={{ duration: 3.5, repeat: Infinity, delay: 1 }}
        style={{ position: 'absolute', bottom: -14, left: -18, background: 'linear-gradient(135deg,#1a2e80,#0d1f5c)', border: '1px solid rgba(0,180,255,0.42)', borderRadius: 14, padding: '9px 14px', boxShadow: '0 0 20px rgba(0,120,255,0.32)', fontSize: 11, color: '#fff', fontWeight: 700, display: 'flex', alignItems: 'center', gap: 7, whiteSpace: 'nowrap' }}>
        <span style={{ fontSize: 16 }}>🧹</span>
        <div><div style={{ fontSize: 8, color: 'rgba(180,210,255,0.6)', marginBottom: 1 }}>AUTO CLEAN</div><div>ENABLED</div></div>
      </motion.div>
    </div>
  );
}

// ── Feature Card ──────────────────────────────────────
function FeatureCard({ icon, title, desc, delay }) {
  const [hov, setHov] = useState(false);
  return (
    <motion.div initial={{ opacity: 0, y: 28 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} transition={{ delay, duration: 0.5 }}
      onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
      style={{ background: hov ? 'linear-gradient(135deg,rgba(0,60,200,0.35),rgba(0,25,90,0.35))' : 'linear-gradient(135deg,rgba(0,25,90,0.5),rgba(4,12,44,0.5))', border: hov ? '1px solid rgba(0,180,255,0.5)' : '1px solid rgba(0,100,200,0.22)', borderRadius: 16, padding: '26px 22px', transition: 'all 0.3s', boxShadow: hov ? '0 0 28px rgba(0,100,255,0.18)' : 'none', display: 'flex', flexDirection: 'column', gap: 13 }}>
      <div style={{ width: 52, height: 52, borderRadius: 13, background: 'linear-gradient(135deg,rgba(0,80,220,0.4),rgba(0,40,140,0.4))', border: '1px solid rgba(0,150,255,0.32)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 22 }}>{icon}</div>
      <h3 style={{ color: '#fff', fontSize: 15, fontWeight: 700, margin: 0, lineHeight: 1.4 }}>{title}</h3>
      <p style={{ color: 'rgba(160,200,255,0.72)', fontSize: 13, margin: 0, lineHeight: 1.7 }}>{desc}</p>
    </motion.div>
  );
}

// ── Stat Card ─────────────────────────────────────────
function StatCard({ value, label, icon }) {
  return (
    <div style={{ background: 'linear-gradient(135deg,rgba(0,40,140,0.6),rgba(0,18,65,0.6))', border: '1px solid rgba(0,150,255,0.28)', borderRadius: 16, padding: '22px 18px', textAlign: 'center', boxShadow: '0 0 22px rgba(0,100,255,0.1)' }}>
      <div style={{ fontSize: 26, marginBottom: 7 }}>{icon}</div>
      <div style={{ fontSize: 30, fontWeight: 900, fontFamily: 'monospace', background: 'linear-gradient(135deg,#00c8ff,#0066ff)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>{value}</div>
      <div style={{ color: 'rgba(160,200,255,0.7)', fontSize: 11, marginTop: 5, letterSpacing: 1 }}>{label}</div>
    </div>
  );
}

// ──────────────────────────────────────────────────────
// 3D PRICING CAROUSEL
// ──────────────────────────────────────────────────────
const PLANS = [
  {
    id: 'trial',
    badge: 'تجربة مجانية', badgeColor: '#22c55e',
    name: 'تجربة مجانية', price: '$0', original: null, period: '/ يومين', saving: null,
    features: ['تشغيل 24 ساعة دون انقطاع', 'نشر تلقائي بضغطة زر واحدة', 'تحديد وقت بقاء الإعلان وحذفه', 'دعم فني وتوجيه مجاني'],
    isPrimary: false, btnText: 'ابدأ التجربة المجانية',
  },
  {
    id: 'monthly',
    badge: '⭐ الأكثر طلباً - وفر $30', badgeColor: '#00c8ff',
    name: 'شهري', price: '$50', original: '$80', period: '/ شهر', saving: 'وفّر $30',
    features: ['نشر في أكثر من 100 قناة بضغطة زر', 'مزامنة المجلدات وحظر القنوات تلقائياً', 'حذف الإعلانات أوتوماتيكياً بدقة ثانية', 'تفعيل استيكر مخصص مانع للحظر'],
    isPrimary: true, btnText: 'تفعيل الباقة الشهرية',
  },
  {
    id: 'half_year',
    badge: 'وفر $210 (وفر 44%)', badgeColor: '#f59e0b',
    name: '6 شهور', price: '$270', original: '$480', period: '/ 6 شهور', saving: 'وفّر $210',
    features: ['توفير كبير بمعدل ($45/شهر فقط)', 'خوادم خاصة فائقة السرعة للعميل', 'دعم كامل لوكيل (Proxy SOCKS5) خاص', 'دعم فني مخصص ومباشر 24/7'],
    isPrimary: false, btnText: 'تفعيل باقة 6 شهور',
  },
  {
    id: 'yearly',
    badge: 'خصم 50% ($480 توفير 👑)', badgeColor: '#f59e0b',
    name: 'سنوي', price: '$480', original: '$960', period: '/ سنة', saving: 'وفّر 50% ($480)',
    features: ['خصم 50% كامل بنصف السعر ($40/شهر)', 'نشر وإدارة قنوات غير محدودة', 'بروكسي SOCKS5 مخصص مجاني مدمج', 'مستشار ومدير حساب مخصص بالكامل'],
    isPrimary: false, btnText: 'تفعيل الباقة السنوية',
  },
];

function Pricing3DCarousel({ onGoToApp, isMobile }) {
  const [active, setActive] = useState(1);
  const touchStart = useRef(null);
  const n = PLANS.length;

  const prev = () => setActive(i => (i - 1 + n) % n);
  const next = () => setActive(i => (i + 1) % n);

  const onTouchStart = (e) => { touchStart.current = e.touches[0].clientX; };
  const onTouchEnd = (e) => {
    if (!touchStart.current) return;
    const diff = touchStart.current - e.changedTouches[0].clientX;
    if (diff > 40) next();
    else if (diff < -40) prev();
    touchStart.current = null;
  };

  // Compute position: 0=center, -1=left, +1=right, others=hidden
  const getPos = (i) => {
    let d = i - active;
    if (d > n / 2) d -= n;
    if (d < -n / 2) d += n;
    return d;
  };

  const getStyle = (pos) => {
    const abs = Math.abs(pos);
    if (abs > 1) return null; // hide beyond 2 slots
    if (pos === 0) return {
      transform: 'translateX(0) scale(1) rotateY(0deg) translateZ(0px)',
      zIndex: 30, opacity: 1, filter: 'none',
    };
    const dir = pos > 0 ? 1 : -1;
    if (isMobile) return {
      transform: `translateX(${dir * 72}%) scale(0.82) rotateY(${dir * -18}deg) translateZ(-60px)`,
      zIndex: 10, opacity: 0.55, filter: 'blur(1px)',
    };
    return {
      transform: `translateX(${dir * 68}%) scale(0.84) rotateY(${dir * -22}deg) translateZ(-80px)`,
      zIndex: 10, opacity: 0.6, filter: 'blur(1px)',
    };
  };

  return (
    <div style={{ width: '100%', maxWidth: 900, margin: '0 auto' }}>
      {/* Dots */}
      <div style={{ display: 'flex', justifyContent: 'center', gap: 8, marginBottom: 32 }}>
        {PLANS.map((_, i) => (
          <button key={i} onClick={() => setActive(i)} style={{ width: i === active ? 30 : 10, height: 10, borderRadius: 5, border: 'none', cursor: 'pointer', background: i === active ? '#0066ff' : 'rgba(0,100,200,0.3)', transition: 'all 0.35s', padding: 0 }} />
        ))}
      </div>

      {/* 3D Stage */}
      <div
        onTouchStart={onTouchStart} onTouchEnd={onTouchEnd}
        style={{ position: 'relative', height: isMobile ? 520 : 500, perspective: '1200px', perspectiveOrigin: '50% 40%' }}
      >
        {PLANS.map((plan, i) => {
          const pos = getPos(i);
          const style = getStyle(pos);
          if (!style) return null;

          return (
            <div key={i} onClick={() => pos !== 0 && setActive(i)}
              style={{
                position: 'absolute', top: 0, left: '50%',
                width: isMobile ? '80%' : '42%',
                minHeight: isMobile ? 440 : 460,
                marginLeft: isMobile ? '-40%' : '-21%',
                transformStyle: 'preserve-3d',
                transition: 'all 0.55s cubic-bezier(0.4,0,0.2,1)',
                cursor: pos !== 0 ? 'pointer' : 'default',
                ...style,
              }}
            >
              {/* Card */}
              <div style={{
                width: '100%', height: '100%',
                background: plan.isPrimary && pos === 0
                  ? 'linear-gradient(135deg,#0041cc,#0028a0)'
                  : pos === 0
                    ? 'linear-gradient(135deg,rgba(0,28,95,0.92),rgba(4,12,45,0.95))'
                    : 'linear-gradient(135deg,rgba(0,18,70,0.7),rgba(2,8,35,0.75))',
                border: plan.isPrimary && pos === 0
                  ? '2px solid rgba(0,200,255,0.65)'
                  : '1px solid rgba(0,100,200,0.28)',
                borderRadius: 22, padding: '30px 24px',
                boxShadow: plan.isPrimary && pos === 0
                  ? '0 0 60px rgba(0,100,255,0.4), 0 20px 60px rgba(0,0,0,0.5)'
                  : '0 8px 40px rgba(0,0,0,0.45)',
                display: 'flex', flexDirection: 'column', gap: 16,
                overflow: 'hidden', position: 'relative',
              }}>
                {/* Top shine */}
                {plan.isPrimary && pos === 0 && (
                  <div style={{ position: 'absolute', top: 0, left: 0, right: 0, height: 2, background: 'linear-gradient(90deg,transparent,#00c8ff,transparent)' }} />
                )}

                {/* Badge */}
                <div style={{ display: 'inline-flex', alignSelf: 'flex-start', background: plan.badgeColor + '22', border: `1px solid ${plan.badgeColor}66`, color: plan.badgeColor, fontSize: 10, fontWeight: 700, padding: '4px 12px', borderRadius: 20 }}>
                  {plan.badge}
                </div>

                {/* Name + Price */}
                <div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10 }}>
                    <div style={{ width: 40, height: 40, borderRadius: 11, background: 'rgba(0,100,255,0.28)', border: '1px solid rgba(0,180,255,0.28)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 18 }}>🛡️</div>
                    <h3 style={{ color: '#fff', fontSize: 17, fontWeight: 700, margin: 0 }}>{plan.name}</h3>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'baseline', gap: 7, flexWrap: 'wrap' }}>
                    {plan.original && <span style={{ textDecoration: 'line-through', color: 'rgba(160,200,255,0.38)', fontSize: 14 }}>{plan.original}</span>}
                    <span style={{ fontSize: 38, fontWeight: 900, color: '#fff', fontFamily: 'Outfit,monospace', lineHeight: 1 }}>{plan.price}</span>
                    <span style={{ color: 'rgba(160,200,255,0.6)', fontSize: 12 }}>{plan.period}</span>
                  </div>
                  {plan.saving && (
                    <div style={{ marginTop: 8, display: 'inline-flex', alignItems: 'center', gap: 5, background: 'rgba(251,191,36,0.12)', border: '1px solid rgba(251,191,36,0.3)', borderRadius: 12, padding: '4px 11px', color: '#fbbf24', fontSize: 11, fontWeight: 700 }}>
                      ✨ {plan.saving}
                    </div>
                  )}
                </div>

                {/* Features */}
                <ul style={{ listStyle: 'none', padding: 0, margin: 0, display: 'flex', flexDirection: 'column', gap: 9 }}>
                  {plan.features.map((f, fi) => (
                    <li key={fi} style={{ display: 'flex', alignItems: 'center', gap: 9, color: 'rgba(200,225,255,0.82)', fontSize: 12.5 }}>
                      <span style={{ color: '#00c8ff', fontSize: 14, flexShrink: 0 }}>✓</span>{f}
                    </li>
                  ))}
                </ul>

                {/* CTA */}
                {pos === 0 && (
                  <button onClick={() => onGoToApp(plan.id)} style={{ marginTop: 'auto', padding: '13px 0', borderRadius: 12, background: plan.isPrimary ? '#fff' : 'linear-gradient(135deg,#0055ee,#0033aa)', border: plan.isPrimary ? 'none' : '1px solid rgba(0,150,255,0.35)', color: plan.isPrimary ? '#0033aa' : '#fff', fontWeight: 700, fontSize: 13, cursor: 'pointer', boxShadow: plan.isPrimary ? '0 4px 20px rgba(255,255,255,0.18)' : '0 4px 14px rgba(0,80,220,0.28)', fontFamily: 'Cairo,sans-serif' }}>
                    {plan.btnText}
                  </button>
                )}
                {pos !== 0 && (
                  <div style={{ marginTop: 'auto', padding: '13px 0', borderRadius: 12, background: 'rgba(0,40,120,0.4)', border: '1px solid rgba(0,100,200,0.2)', color: 'rgba(160,200,255,0.5)', fontWeight: 600, fontSize: 12, textAlign: 'center', fontFamily: 'Cairo,sans-serif' }}>
                    اضغط للعرض
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {/* Arrow Buttons */}
      <div style={{ display: 'flex', justifyContent: 'center', gap: 16, marginTop: 32 }}>
        <button onClick={prev} style={{ width: 52, height: 52, borderRadius: '50%', background: 'rgba(0,60,180,0.4)', border: '1px solid rgba(0,140,255,0.35)', color: '#00c8ff', fontWeight: 700, fontSize: 20, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', transition: 'all 0.2s' }}>→</button>
        <button onClick={next} style={{ width: 52, height: 52, borderRadius: '50%', background: 'rgba(0,60,180,0.4)', border: '1px solid rgba(0,140,255,0.35)', color: '#00c8ff', fontWeight: 700, fontSize: 20, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', transition: 'all 0.2s' }}>←</button>
      </div>
    </div>
  );
}

// ── Mobile Drawer ─────────────────────────────────────
function MobileMenu({ open, onClose, goToApp, goToFeatures, goToPricing }) {
  return (
    <AnimatePresence>
      {open && (
        <>
          <motion.div key="ov" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
            onClick={onClose} style={{ position: 'fixed', inset: 0, background: 'rgba(0,4,18,0.78)', zIndex: 200, backdropFilter: 'blur(5px)' }} />
          <motion.div key="dr" initial={{ x: '100%' }} animate={{ x: 0 }} exit={{ x: '100%' }} transition={{ type: 'spring', damping: 22, stiffness: 180 }}
            style={{ position: 'fixed', top: 0, right: 0, width: 280, height: '100%', background: 'linear-gradient(160deg,#030f28,#061530)', borderLeft: '1px solid rgba(0,100,220,0.25)', zIndex: 201, display: 'flex', flexDirection: 'column', padding: '28px 24px', gap: 8 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 28 }}>
              <img src="logo.png" alt="AutoTele" style={{ height: 48, objectFit: 'contain', filter: 'drop-shadow(0 0 10px rgba(0,200,255,0.85)) brightness(1.4)' }} />
              <button onClick={onClose} style={{ background: 'none', border: 'none', color: 'rgba(160,200,255,0.8)', fontSize: 24, cursor: 'pointer' }}>✕</button>
            </div>
            {[
              { label: 'الرئيسية', action: () => { window.scrollTo({ top: 0, behavior: 'smooth' }); onClose(); } },
              { label: 'المميزات', action: () => { goToFeatures(); onClose(); } },
              { label: 'خطط الأسعار', action: () => { goToPricing(); onClose(); } },
            ].map(item => (
              <button key={item.label} onClick={item.action}
                style={{ width: '100%', padding: '15px 18px', background: 'rgba(0,40,130,0.3)', border: '1px solid rgba(0,100,200,0.2)', borderRadius: 12, color: '#fff', fontWeight: 600, fontSize: 15, cursor: 'pointer', textAlign: 'right', fontFamily: 'Cairo,sans-serif' }}>
                {item.label}
              </button>
            ))}
            <button onClick={() => { goToApp(); onClose(); }}
              style={{ marginTop: 16, width: '100%', padding: '15px 18px', background: 'linear-gradient(135deg,#0055ee,#0033aa)', border: 'none', borderRadius: 12, color: '#fff', fontWeight: 700, fontSize: 15, cursor: 'pointer', fontFamily: 'Cairo,sans-serif', boxShadow: '0 0 25px rgba(0,100,255,0.4)' }}>
              تسجيل الدخول
            </button>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  );
}

// ── What You Get Inside Section ───────────────────────
function CommandShowcaseCard({ cmd, title, desc, icon, isMobile }) {
  const [hov, setHov] = useState(false);
  return (
    <div
      onMouseEnter={() => setHov(true)} onMouseLeave={() => setHov(false)}
      style={{
        background: hov ? 'linear-gradient(135deg,rgba(0,110,255,0.18),rgba(0,30,120,0.22))' : 'linear-gradient(135deg,rgba(0,35,110,0.12),rgba(1,10,38,0.4))',
        border: hov ? '1px solid rgba(0,210,255,0.6)' : '1px solid rgba(0,120,220,0.22)',
        borderRadius: 20, padding: '24px 20px', transition: 'all 0.3s',
        boxShadow: hov ? '0 0 35px rgba(0,180,255,0.25), inset 0 0 15px rgba(0,180,255,0.05)' : 'none',
        display: 'flex', flexDirection: 'column', gap: 12, textAlign: 'right', position: 'relative', overflow: 'hidden',
        height: '100%',
        boxSizing: 'border-box'
      }}
    >
      <div style={{ position: 'absolute', top: 0, right: 0, width: 60, height: 60, background: 'radial-gradient(circle, rgba(0,200,255,0.12) 0%, transparent 70%)', pointerEvents: 'none' }} />
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 4 }}>
        <div style={{ width: 44, height: 44, borderRadius: 10, background: 'rgba(0,100,255,0.18)', border: '1px solid rgba(0,180,255,0.3)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 18 }}>{icon}</div>
        <span style={{ fontFamily: 'monospace', fontSize: 11, fontWeight: 700, color: '#00c8ff', background: 'rgba(0,200,255,0.12)', border: '1px solid rgba(0,200,255,0.25)', padding: '3px 10px', borderRadius: 6, letterSpacing: 0.5 }}>{cmd}</span>
      </div>
      <h3 style={{ color: '#fff', fontSize: 15, fontWeight: 800, margin: 0, fontFamily: 'Cairo,sans-serif' }}>{title}</h3>
      <p style={{ color: 'rgba(175,205,255,0.74)', fontSize: 12.5, margin: 0, lineHeight: 1.65, fontFamily: 'Cairo,sans-serif' }}>{desc}</p>
    </div>
  );
}

function WhatYouGetInside({ isMobile }) {
  const commands = [
    { cmd: '.يلا', title: 'أمر التبادل السحابي التلقائي', desc: 'أطلق موجات النشر التبادلي السحابي فوراً أو بجدولة مؤجلة. ينسق النشر بين قنواتك بشكل ثنائي متكافئ لمنع الحظر ورفع التفاعل.', icon: '🔄' },
    { cmd: '.حملة', title: 'الحملات الفردية ودمج الروابط', desc: 'انشر إعلاناً ترويجياً واحداً أو عدة روابط ترويجية مدمجة دفعة واحدة في أكثر من 100 قناة وجروب بضغطة زر وبثوانٍ معدودة.', icon: '📢' },
    { cmd: '.حملات', title: 'نشر مجلدات تليجرام المجمعة', desc: 'استهدف قنوات مجلد "حملات" بالكامل ونشر الإعلانات بداخلها بفواصل زمنية مرنة لحماية الحساب من قيود السبام تلقائياً.', icon: '📁' },
    { cmd: '.مسح', title: 'ممحاة الإعلانات الذكية', desc: 'نظف قنواتك من إعلانات التبادل المنتهية فوراً. يمسح الإعلانات والملصقات القديمة بدقة ويحافظ على جاذبية محتوى قناتك.', icon: '🧹' },
    { cmd: '.اولويات', title: 'فرز القنوات حسب المشاهدات', desc: 'يصنف قنواتك وجروباتك تلقائياً ويعرضها مرتبة بناءً على عدد المشاهدات الحية وعدد الأعضاء لتحديد قنواتك الأكثر فاعلية بلمحة.', icon: '📊' },
    { cmd: '.المهام', title: 'مراقبة طابور النشر المجدول', desc: 'يعرض لك المهام القائمة والمستقبلية المجدولة من الموقع أو عبر تليجرام، مع تتبع حالة التبادل والعداد التنازلي التفاعلي المباشر.', icon: '⏳' },
    { cmd: '.تحديث', title: 'مزامنة المجلدات والكاش لحظياً', desc: 'تحديث فوري لقائمة قنواتك والجروبات ومجلدات الاستثناء والحظر الرسمية في تليجرام لمزامنتها سحابياً وتعديلها بالريديس.', icon: '🔄' },
    { cmd: '.تثبيت', title: 'توجيه الأعضاء والتثبيت التلقائي', desc: 'نشر منشور ترويجي مخصص وتثبيته في أعلى القنوات الحاضنة لتوجيه حركة الزوار والأعضاء الجدد نحو قناتك المستهدفة بكفاءة.', icon: '📌' },
  ];

  const marqueeCommands = [...commands, ...commands];

  return (
    <section style={{ position: 'relative', zIndex: 1, padding: '80px 0', borderTop: '1px solid rgba(0,80,180,0.18)', overflow: 'hidden' }}>
      <style>{`
        @keyframes marquee-scroll {
          0% { transform: translate3d(0, 0, 0); }
          100% { transform: translate3d(-50%, 0, 0); }
        }
        .marquee-container {
          position: relative;
          width: 100%;
          overflow: hidden;
          padding: 20px 0;
          direction: ltr;
        }
        .marquee-track {
          display: flex;
          gap: 20px;
          width: max-content;
          animation: marquee-scroll 35s linear infinite;
        }
        .marquee-track:hover {
          animation-play-state: paused;
        }
        .marquee-overlay-left {
          position: absolute;
          top: 0;
          left: 0;
          bottom: 0;
          width: 150px;
          background: linear-gradient(to right, #020d1f 15%, transparent);
          z-index: 5;
          pointer-events: none;
        }
        .marquee-overlay-right {
          position: absolute;
          top: 0;
          right: 0;
          bottom: 0;
          width: 150px;
          background: linear-gradient(to left, #020d1f 15%, transparent);
          z-index: 5;
          pointer-events: none;
        }
        @media (max-width: 768px) {
          .marquee-overlay-left { width: 50px; }
          .marquee-overlay-right { width: 50px; }
          .marquee-track {
            animation: marquee-scroll 25s linear infinite;
          }
        }
      `}</style>

      <div style={{ maxWidth: 1100, margin: '0 auto', padding: '0 20px' }}>
        <div style={{ textAlign: 'center', marginBottom: 36 }}>
          <div style={{ display: 'inline-block', background: 'rgba(0,60,200,0.22)', border: '1px solid rgba(0,150,255,0.32)', padding: '7px 18px', borderRadius: 30, color: '#00c8ff', fontSize: 12, fontWeight: 700, letterSpacing: 2, marginBottom: 18, fontFamily: 'Cairo,sans-serif' }}>
            📦 ماذا ستجد في الداخل؟ (What You Get)
          </div>
          <h2 style={{ fontSize: isMobile ? 24 : 38, fontWeight: 800, margin: '0 0 14px', lineHeight: 1.3, fontFamily: 'Cairo,sans-serif' }}>
            لوحة تحكم كاملة في جيبك عبر رسائل تليجرام المحفوظة
          </h2>
          <p style={{ color: 'rgba(160,200,255,0.68)', maxWidth: 680, margin: '0 auto', fontSize: 14, lineHeight: 1.8, fontFamily: 'Cairo,sans-serif' }}>
            بدلاً من الواجهات المعقدة، يوفر لك AutoTele محرك أوامر ذكي وتفاعلي للتحكم بجميع قنواتك مباشرة من هاتفك، مع حزمة متكاملة تلبي احتياجاتك.
          </p>
        </div>
      </div>

      <div className="marquee-container">
        <div className="marquee-overlay-left" />
        <div className="marquee-overlay-right" />
        <div className="marquee-track">
          {marqueeCommands.map((c, idx) => (
            <div key={idx} style={{ width: isMobile ? '280px' : '340px', flexShrink: 0 }}>
              <CommandShowcaseCard cmd={c.cmd} title={c.title} desc={c.desc} icon={c.icon} isMobile={isMobile} />
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function DemoVideoSection({ isMobile }) {
  const [isPlaying, setIsPlaying] = React.useState(false);
  const cropTop = isMobile ? 44 : 60;
  
  return (
    <section id="demo-section" style={{ position: 'relative', zIndex: 1, padding: isMobile ? '60px 20px' : '100px 20px', borderTop: '1px solid rgba(0,80,180,0.18)', background: 'linear-gradient(180deg,#020d1f 0%,rgba(0,18,72,0.15) 100%)' }}>
      <div style={{ maxWidth: 1200, margin: '0 auto', padding: '0 20px' }}>
        <div style={{ display: 'grid', gridTemplateColumns: isMobile ? '1fr' : '1fr 1fr', gap: isMobile ? 40 : 60, alignItems: 'center' }}>
          
          {/* Right Side: Text & Promo Content */}
          <motion.div 
            initial={{ opacity: 0, x: isMobile ? 0 : -35, y: isMobile ? 20 : 0 }} 
            whileInView={{ opacity: 1, x: 0, y: 0 }} 
            viewport={{ once: true }} 
            transition={{ duration: 0.6 }}
            style={{ display: 'flex', flexDirection: 'column', gap: 20, textAlign: 'right' }}
          >
            <div style={{ display: 'inline-flex', alignItems: 'center', gap: 8, background: 'rgba(0,60,200,0.22)', border: '1px solid rgba(0,150,255,0.32)', padding: '7px 18px', borderRadius: 30, color: '#00c8ff', fontSize: 12, fontWeight: 700, alignSelf: 'flex-start' }}>
              🎥 عرض توضيحي للمشروع
            </div>
            <h2 style={{ fontSize: isMobile ? 26 : 38, fontWeight: 900, lineHeight: 1.3, margin: 0, fontFamily: 'Cairo,sans-serif' }}>
              اترك موبايلك وحافظ على وقتك معنا ⏱️
            </h2>
            <p style={{ color: 'rgba(160,200,255,0.78)', fontSize: 14, lineHeight: 1.8, margin: 0, fontFamily: 'Cairo,sans-serif' }}>
              شاهد هذا العرض التوضيحي السريع لترى كيف يقوم المحرك السحابي الذكي AutoTele بإدارة وتنسيق ونشر حملات التبادل الإعلاني وقنوات تليجرام تلقائياً بالكامل بالنيابة عنك دون تدخل يدوي، لتتفرغ تماماً لما يهمك!
            </p>
            
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12, marginTop: 10 }}>
              {[
                { title: 'وفر وقتك بالكامل:', desc: 'لا داعي للبقاء متيقظاً طوال اليوم لنشر وحذف الإعلانات يدوياً.' },
                { title: 'أتمتة ذكية ومجدولة:', desc: 'البوت ينفذ المهام بذكاء وسرعة فائقة من السيرفر مباشرة.' },
                { title: 'أمان كامل ضد الحظر:', desc: 'فواصل زمنية ذكية وملصقات مخصصة لتفادي قيود تليجرام.' }
              ].map((item, i) => (
                <div key={i} style={{ display: 'flex', gap: 12, background: 'rgba(0,40,140,0.12)', border: '1px solid rgba(0,100,255,0.12)', borderRadius: 12, padding: '14px 18px', textAlign: 'right' }}>
                  <div style={{ fontSize: 20, flexShrink: 0 }}>✨</div>
                  <div>
                    <h4 style={{ color: '#fff', fontSize: 14, fontWeight: 700, margin: '0 0 4px', fontFamily: 'Cairo,sans-serif' }}>{item.title}</h4>
                    <p style={{ color: 'rgba(160,200,255,0.65)', fontSize: 12, margin: 0, lineHeight: 1.5, fontFamily: 'Cairo,sans-serif' }}>{item.desc}</p>
                  </div>
                </div>
              ))}
            </div>
          </motion.div>

          {/* Left Side: Premium Video Player Card */}
          <div style={{ width: '100%', maxWidth: isMobile ? '550px' : 'none', margin: '0 auto' }}>
            <motion.div 
              initial={{ opacity: 0, x: isMobile ? 0 : 35, y: isMobile ? 20 : 0 }} 
              whileInView={{ opacity: 1, x: 0, y: 0 }} 
              viewport={{ once: true }} 
              transition={{ duration: 0.6, delay: 0.15 }}
              style={{ position: 'relative', width: '100%', borderRadius: 24, overflow: 'hidden', border: '1.5px solid rgba(0,140,255,0.45)', boxShadow: '0 0 40px rgba(0,100,255,0.25)', background: '#000' }}
            >
              <div style={{ width: '100%', paddingTop: '56.25%', position: 'relative', overflow: 'hidden' }}>
                {!isPlaying ? (
                  // Thumbnail / Play Trigger View
                  <div 
                    onClick={() => setIsPlaying(true)}
                    style={{ position: 'absolute', inset: 0, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'radial-gradient(circle at center, rgba(9,24,64,0.6) 0%, rgba(2,13,31,0.95) 100%)' }}
                  >
                    {/* Glowing Play Button */}
                    <motion.div 
                      animate={{ scale: [1, 1.1, 1] }} 
                      transition={{ duration: 2, repeat: Infinity, ease: 'easeInOut' }}
                      style={{ width: 80, height: 80, borderRadius: '50%', background: 'linear-gradient(135deg,#00c8ff,#0066ff)', border: '2px solid rgba(255,255,255,0.8)', display: 'flex', alignItems: 'center', justifyContent: 'center', boxShadow: '0 0 30px rgba(0,180,255,0.6)', zIndex: 10 }}
                    >
                      <svg viewBox="0 0 24 24" width="36" height="36" fill="#fff"><path d="M8 5v14l11-7z"/></svg>
                    </motion.div>
                    
                    {/* Decorative Logo / Graphics behind Play */}
                    <div style={{ position: 'absolute', top: '20px', left: '20px', display: 'flex', alignItems: 'center', gap: 8, opacity: 0.8 }}>
                      <img src="logo.png" alt="AutoTele Logo" style={{ width: 32, height: 32, objectFit: 'contain' }} />
                      <span style={{ fontSize: 12, fontWeight: 700, letterSpacing: 1, fontFamily: 'Cairo,sans-serif' }}>عرض مشروع AUTOTELE</span>
                    </div>
                  </div>
                ) : (
                  // Embedded YouTube Video Player Iframe with cropping wrapper
                  <div style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', overflow: 'hidden' }}>
                    <iframe 
                      style={{ position: 'absolute', top: `-${cropTop}px`, left: 0, width: '100%', height: `calc(100% + ${cropTop}px)`, border: 'none' }}
                      src="https://www.youtube.com/embed/6AgFaRE3T8M?autoplay=1&rel=0&showinfo=0&controls=1&modestbranding=1&iv_load_policy=3" 
                      title="AutoTele Demo Video" 
                      frameBorder="0" 
                      allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" 
                      allowFullScreen
                    ></iframe>
                  </div>
                )}
              </div>
            </motion.div>
          </div>

        </div>
      </div>
    </section>
  );
}

// ── MAIN ──────────────────────────────────────────────
function AutoTeleLanding() {
  const [menuOpen, setMenuOpen] = useState(false);
  const [isMobile, setIsMobile] = useState(window.innerWidth < 900);

  useEffect(() => {
    const loader = document.getElementById('loading-screen');
    if (loader && window.triggerOpen) {
      setTimeout(() => {
        window.triggerOpen();
      }, 1000);
    } else if (loader) {
      loader.style.opacity = '0';
      setTimeout(() => loader.style.display = 'none', 500);
    }
    const r = () => setIsMobile(window.innerWidth < 900);
    window.addEventListener('resize', r);
    return () => window.removeEventListener('resize', r);
  }, []);

  const goToApp      = (planKey) => {
    if (planKey) {
      window.location.href = `app.html?plan=${planKey}`;
    } else {
      window.location.href = 'app.html';
    }
  };
  const goToPricing  = () => document.getElementById('pricing-section')?.scrollIntoView({ behavior: 'smooth' });
  const goToFeatures = () => document.getElementById('features-section')?.scrollIntoView({ behavior: 'smooth' });

  const lnk = { color: 'rgba(180,210,255,0.85)', fontSize: 14, fontWeight: 600, textDecoration: 'none', cursor: 'pointer' };

  return (
    <div style={{ minHeight: '100vh', background: '#020d1f', color: '#fff', fontFamily: "'Cairo','Tajawal',sans-serif", overflowX: 'hidden', position: 'relative' }}>

      {/* Global glows */}
      <div style={{ position: 'fixed', inset: 0, pointerEvents: 'none', zIndex: 0 }}>
        <div style={{ position: 'absolute', top: '-10%', left: '50%', transform: 'translateX(-50%)', width: '80vw', height: '60vh', background: 'radial-gradient(ellipse at center,rgba(0,60,200,0.2) 0%,transparent 70%)', borderRadius: '50%' }} />
        <div style={{ position: 'absolute', bottom: '10%', right: 0, width: '50vw', height: '50vh', background: 'radial-gradient(ellipse at right,rgba(0,40,160,0.16) 0%,transparent 70%)' }} />
      </div>

      <MobileMenu open={menuOpen} onClose={() => setMenuOpen(false)} goToApp={goToApp} goToFeatures={goToFeatures} goToPricing={goToPricing} />

      {/* ── NAVBAR ── */}
      <header style={{ position: 'sticky', top: 0, zIndex: 100, background: 'rgba(2,13,31,0.9)', backdropFilter: 'blur(22px)', borderBottom: '1px solid rgba(0,100,200,0.18)', boxShadow: '0 4px 28px rgba(0,50,200,0.1)' }}>
        <div style={{ maxWidth: 1200, margin: '0 auto', padding: '0 20px', height: 72, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>

          {/* ★ Logo — big & clear */}
          <div onClick={() => window.scrollTo({ top: 0, behavior: 'smooth' })} style={{ cursor: 'pointer', flexShrink: 0 }}>
            <img src="logo.png" alt="AutoTele"
              style={{
                height: isMobile ? 52 : 64,
                width: 'auto',
                objectFit: 'contain',
                filter: 'drop-shadow(0 0 16px rgba(0,210,255,1)) drop-shadow(0 0 32px rgba(0,120,255,0.6)) brightness(1.5)',
                display: 'block',
              }} />
          </div>

          {/* Desktop nav */}
          {!isMobile && (
            <nav style={{ display: 'flex', gap: 36, alignItems: 'center' }}>
              <a href="#" style={lnk} onClick={() => window.scrollTo({ top: 0, behavior: 'smooth' })}>الرئيسية</a>
              <a href="#features-section" style={lnk} onClick={goToFeatures}>المميزات</a>
              <a href="#pricing-section" style={lnk} onClick={goToPricing}>خطط الأسعار</a>
            </nav>
          )}

          {isMobile ? (
            <button onClick={() => setMenuOpen(true)}
              style={{ background: 'rgba(0,60,180,0.35)', border: '1px solid rgba(0,140,255,0.35)', borderRadius: 10, padding: '10px 13px', cursor: 'pointer', display: 'flex', flexDirection: 'column', gap: 5 }}>
              {[0, 1, 2].map(i => <span key={i} style={{ display: 'block', width: 22, height: 2, background: '#00c8ff', borderRadius: 2 }} />)}
            </button>
          ) : (
            <button onClick={goToApp} style={{ padding: '10px 26px', borderRadius: 30, background: 'linear-gradient(135deg,#0055ee,#003dbb)', border: '1px solid rgba(0,180,255,0.45)', color: '#fff', fontWeight: 700, fontSize: 13, cursor: 'pointer', boxShadow: '0 0 22px rgba(0,100,255,0.35)', letterSpacing: 0.5 }}>
              تسجيل الدخول
            </button>
          )}
        </div>
      </header>

      {/* ── HERO ── */}
      <section style={{ position: 'relative', zIndex: 1, minHeight: '88vh', display: 'flex', alignItems: 'center', padding: '60px 20px 80px' }}>
        <ParticleBg />
        <div style={{ maxWidth: 1200, margin: '0 auto', width: '100%', display: 'grid', gridTemplateColumns: isMobile ? '1fr' : '1fr 1fr', gap: isMobile ? 48 : 60, alignItems: 'center' }}>
          <motion.div initial={{ opacity: 0, x: -36 }} animate={{ opacity: 1, x: 0 }} transition={{ duration: 0.7 }}
            style={{ display: 'flex', flexDirection: 'column', gap: 22, textAlign: 'right' }}>
            <motion.div initial={{ opacity: 0, y: -16 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.1 }}
              style={{ display: 'inline-flex', alignItems: 'center', gap: 8, background: 'rgba(0,60,200,0.24)', border: '1px solid rgba(0,150,255,0.33)', padding: '7px 16px', borderRadius: 30, alignSelf: 'flex-start', color: '#00c8ff', fontSize: 12, fontWeight: 700 }}>
              🚀 الجيل الجديد من أتمتة التليجرام السحابية
            </motion.div>
            <motion.h1 initial={{ opacity: 0, y: 28 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.2 }}
              style={{ fontSize: isMobile ? 28 : 50, fontWeight: 900, lineHeight: 1.25, margin: 0 }}>
              إدارة وأتمتة{' '}
              <span style={{ background: 'linear-gradient(135deg,#00c8ff,#0066ff)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>قنوات تليجرام</span>
              <br />على مدار الساعة 24/7
            </motion.h1>
            <motion.p initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.35 }}
              style={{ fontSize: 14, color: 'rgba(160,200,255,0.78)', lineHeight: 1.8, margin: 0 }}>
              ضاعف نمو شبكتك ونسّق حملات التبادل الإعلاني والنشر التلقائي على مدار الساعة. محركنا الذكي ينشر الإعلانات ويمسحها تلقائياً.
            </motion.p>
            <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ delay: 0.42 }}
              style={{ display: 'flex', flexDirection: 'column', gap: 9 }}>
              {['توفير 90%+ من وقتك بأتمتة كاملة', 'نشر متوازي لعشرات القنوات بضغطة واحدة', 'مسح تلقائي فور انتهاء مدة الإعلان'].map((t, i) => (
                <div key={i} style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                  <div style={{ width: 20, height: 20, borderRadius: '50%', background: 'rgba(0,100,255,0.22)', border: '1px solid rgba(0,180,255,0.38)', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#00c8ff', fontSize: 10, flexShrink: 0 }}>✓</div>
                  <span style={{ color: 'rgba(200,225,255,0.82)', fontSize: 13 }}>{t}</span>
                </div>
              ))}
            </motion.div>
            <motion.div initial={{ opacity: 0, y: 18 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: 0.52 }}
              style={{ display: 'flex', gap: 12, flexWrap: 'wrap' }}>
              <button onClick={() => goToApp('trial')} style={{ padding: '13px 30px', borderRadius: 30, background: 'linear-gradient(135deg,#0066ff,#003dbb)', border: 'none', color: '#fff', fontWeight: 800, fontSize: 14, cursor: 'pointer', boxShadow: '0 0 28px rgba(0,100,255,0.5)' }}>
                ابدأ تجربتك المجانية ←
              </button>
              <button onClick={goToPricing} style={{ padding: '13px 26px', borderRadius: 30, background: 'transparent', border: '1.5px solid rgba(0,150,255,0.42)', color: '#00c8ff', fontWeight: 700, fontSize: 14, cursor: 'pointer' }}>
                عرض خطط الأسعار
              </button>
            </motion.div>
          </motion.div>
          <motion.div initial={{ opacity: 0, x: 36 }} animate={{ opacity: 1, x: 0 }} transition={{ duration: 0.7, delay: 0.2 }}>
            <HeroGraphic />
          </motion.div>
        </div>
      </section>

      {/* ── STATS ── */}
      <section style={{ position: 'relative', zIndex: 1, background: 'linear-gradient(180deg,rgba(0,28,95,0.38) 0%,rgba(0,14,52,0.48) 100%)', borderTop: '1px solid rgba(0,100,200,0.18)', borderBottom: '1px solid rgba(0,100,200,0.18)', padding: '48px 20px' }}>
        <div style={{ maxWidth: 1100, margin: '0 auto', display: 'grid', gridTemplateColumns: isMobile ? '1fr 1fr' : 'repeat(4,1fr)', gap: 18 }}>
          <StatCard value="+1,200" label="قناة مؤتمتة" icon="📡" />
          <StatCard value="+5M" label="إعلان منشور" icon="📢" />
          <StatCard value="99.9%" label="معدل استقرار" icon="⚡" />
          <StatCard value="+450" label="شبكة نشطة" icon="🌐" />
        </div>
      </section>

      {/* ── FEATURES ── */}
      <section id="features-section" style={{ position: 'relative', zIndex: 1, padding: '80px 20px' }}>
        <div style={{ maxWidth: 1100, margin: '0 auto' }}>
          <div style={{ textAlign: 'center', marginBottom: 52 }}>
            <motion.div initial={{ opacity: 0, y: 18 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }}
              style={{ display: 'inline-block', background: 'rgba(0,60,200,0.22)', border: '1px solid rgba(0,150,255,0.32)', padding: '7px 18px', borderRadius: 30, color: '#00c8ff', fontSize: 12, fontWeight: 700, letterSpacing: 2, marginBottom: 18, textTransform: 'uppercase' }}>
              🛠️ أدوات متكاملة وقوية
            </motion.div>
            <motion.h2 initial={{ opacity: 0, y: 18 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }} transition={{ delay: 0.1 }}
              style={{ fontSize: isMobile ? 24 : 38, fontWeight: 800, margin: '0 0 14px', lineHeight: 1.3 }}>
              كفاءة وأمان لا مثيل لهما
            </motion.h2>
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: isMobile ? '1fr' : 'repeat(2,1fr)', gap: 20 }}>
            <FeatureCard delay={0} icon="🚀" title="موجات النشر المجدولة (.يلا)" desc="أطلق حملات التبادل التلقائي بين قنواتك. يعمل المحرك سحابياً دون تواجدك ويدير كل شيء بدقة متناهية." />
            <FeatureCard delay={0.1} icon="🧹" title="المسح التلقائي الذكي" desc="يراقب الإعلانات المنشورة ويحذفها فور انتهاء مدتها، مما يبقي قنواتك نظيفة ومنظمة دائماً." />
            <FeatureCard delay={0.2} icon="📁" title="دعم مجلدات تليجرام" desc="يزامن مجلداتك الرسمية (حملات، استثناء، حظر) تلقائياً. أضف قناة من هاتفك وتحدث البوت فوراً." />
            <FeatureCard delay={0.3} icon="📊" title="سجلات أحداث مباشرة" desc="راقب نشاط المحرك لحظة بلحظة. احصل على كل سجلات النشر والمسح مباشرة من لوحة تحكمك." />
          </div>
        </div>
      </section>

      <WhatYouGetInside isMobile={isMobile} />

      <DemoVideoSection isMobile={isMobile} />

      {/* ── PRICING 3D ── */}
      <section id="pricing-section" style={{ position: 'relative', zIndex: 1, padding: '80px 20px', background: 'linear-gradient(180deg,rgba(0,18,72,0.3) 0%,rgba(0,9,36,0.4) 100%)', borderTop: '1px solid rgba(0,80,180,0.18)' }}>
        <div style={{ maxWidth: 1100, margin: '0 auto' }}>
          <div style={{ textAlign: 'center', marginBottom: 48 }}>
            <div style={{ display: 'inline-block', background: 'rgba(0,60,200,0.22)', border: '1px solid rgba(0,150,255,0.32)', padding: '7px 18px', borderRadius: 30, color: '#00c8ff', fontSize: 12, fontWeight: 700, letterSpacing: 2, marginBottom: 18 }}>💎 خطط مرنة وبسيطة</div>
            <h2 style={{ fontSize: isMobile ? 24 : 38, fontWeight: 800, margin: '0 0 14px' }}>خطط الأسعار والعروض</h2>
            <p style={{ color: 'rgba(160,200,255,0.68)', maxWidth: 480, margin: '0 auto', fontSize: 14 }}>
              من التجربة المجانية إلى الباقة السنوية — اقلب بين الخطط يميناً وشمالاً.
            </p>
          </div>
          <Pricing3DCarousel onGoToApp={goToApp} isMobile={isMobile} />
        </div>
      </section>

      {/* ── CTA with VIDEO BACKGROUND ── */}
      <section style={{ position: 'relative', zIndex: 1, overflow: 'hidden', padding: '100px 20px', textAlign: 'center' }}>
        {/* Video Background */}
        <video
          autoPlay muted loop playsInline
          style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', objectFit: 'cover', zIndex: 0 }}
          onError={(e) => { e.target.style.display = 'none'; }}
        >
          <source src="hero.mp4" type="video/mp4" />
          <source src="hero.webm" type="video/webm" />
        </video>

        {/* Dark overlay on video */}
        <div style={{ position: 'absolute', inset: 0, background: 'linear-gradient(135deg,rgba(1,8,32,0.82) 0%,rgba(0,20,80,0.72) 50%,rgba(1,8,32,0.82) 100%)', zIndex: 1 }} />

        {/* Glow overlay */}
        <div style={{ position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%,-50%)', width: '70%', height: '100%', background: 'radial-gradient(ellipse at center,rgba(0,80,255,0.22) 0%,transparent 70%)', pointerEvents: 'none', zIndex: 2 }} />

        {/* Content */}
        <motion.div initial={{ opacity: 0, y: 28 }} whileInView={{ opacity: 1, y: 0 }} viewport={{ once: true }}
          style={{ maxWidth: 680, margin: '0 auto', position: 'relative', zIndex: 3 }}>
          <div style={{ display: 'inline-block', background: 'rgba(0,80,220,0.35)', border: '1px solid rgba(0,200,255,0.45)', padding: '7px 20px', borderRadius: 30, color: '#00c8ff', fontSize: 12, fontWeight: 700, letterSpacing: 2, marginBottom: 24 }}>🚀 انطلق معنا اليوم</div>
          <h2 style={{ fontSize: isMobile ? 28 : 50, fontWeight: 900, lineHeight: 1.25, marginBottom: 20 }}>
            هل أنت جاهز لتوفير وقتك<br />
            <span style={{ background: 'linear-gradient(135deg,#00c8ff,#0066ff)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>ونمو قنواتك؟</span>
          </h2>
          <p style={{ color: 'rgba(200,225,255,0.8)', fontSize: 15, lineHeight: 1.8, marginBottom: 38 }}>
            انضم إلى مئات المديرين وأصحاب القنوات الذين وثقوا في AutoTele. ابدأ تجربتك المجانية الآن دون أي التزامات.
          </p>
          <button onClick={() => goToApp('trial')}
            style={{ padding: '16px 48px', borderRadius: 50, background: 'linear-gradient(135deg,#0066ff,#003dbb)', border: '1px solid rgba(0,200,255,0.45)', color: '#fff', fontWeight: 800, fontSize: 16, cursor: 'pointer', boxShadow: '0 0 60px rgba(0,100,255,0.65),0 0 100px rgba(0,60,200,0.3)' }}>
            ابدأ تجربتك المجانية ←
          </button>
        </motion.div>
      </section>

      {/* ── FOOTER ── */}
      <footer style={{ position: 'relative', zIndex: 1, background: '#010a1a', borderTop: '1px solid rgba(0,80,180,0.22)', padding: '28px 20px', display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', alignItems: 'center', gap: 14 }}>
        <img src="logo.png" alt="AutoTele"
          style={{ height: 46, width: 'auto', objectFit: 'contain', filter: 'drop-shadow(0 0 10px rgba(0,200,255,0.7)) brightness(1.35)', display: 'block' }} />
        <p style={{ color: 'rgba(100,140,200,0.48)', fontSize: 11, fontFamily: 'monospace', margin: 0 }}>
          © 2026 AUTOTELE — ALL RIGHTS RESERVED
        </p>
      </footer>

    </div>
  );
}

ReactDOM.createRoot(document.getElementById('landing-root')).render(<AutoTeleLanding />);
