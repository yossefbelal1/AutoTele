#!/bin/bash
# سكربت الإصلاح الذاتي والتنظيف الدوري لسيرفر TelegAuto SaaS
echo "=== بدء عملية التنظيف الدوري للقرص وقاعدة البيانات ($(date)) ==="

# 1. تنظيف كاش دوكر والنسخ التالفة أو غير المستخدمة
docker system prune -a -f --volumes

# 2. تنظيف وتطهير قاعدة البيانات من سجلات النشر والإشعارات القديمة (أكبر من 30 يوماً)
docker exec -i saas_postgres psql -U postgres -d ad_exchange -c "
  DELETE FROM publish_logs WHERE created_at < NOW() - INTERVAL '30 days';
  DELETE FROM subscription_notification_logs WHERE sent_at < NOW() - INTERVAL '30 days';
"

# 3. تنظيف ملفات النظام المؤقتة
find /tmp -type f -atime +2 -delete

echo "=== انتهت عملية التنظيف والتعافي بنجاح! ==="
