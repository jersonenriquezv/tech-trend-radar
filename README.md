# Tech Trend Radar v1 🚀

Un sistema para recolectar y organizar **tendencias tecnológicas** en tiempo real desde múltiples fuentes.  
El objetivo es ayudar a generar **ideas de contenido** y tener un radar claro de qué está pasando en la industria tech.  

---

## 📌 Estado actual (v1)
- **Base de datos** en SQLite (`core/db.py`) para guardar eventos sin duplicados.  
- **Matcher** (`core/matcher.py`) para filtrar y mapear eventos a topics definidos en `config/topics.json`.  
- **Cache** (`core/cache.py`) para evitar consultas repetidas y respetar rate limits.  
- **Collectors implementados**:
  - ✅ GitHub → busca repositorios recientes por keyword.  
  - ✅ Hacker News → busca historias recientes en top/new por keyword.  
  - 🔜 Reddit → subreddits técnicos (programming, devops, dataengineering, etc.).  
  - 🔜 Product Hunt → categoría Developer Tools.  
-  **Orquestador (`run_once.py`)**
    - Carga `.env`  
    - Selecciona topics del JSON  
    - Llama collectors con caching  
    - Filtra con matcher  
    - Inserta en DB  

---

## 🎯 Próximos pasos
1. Terminar collectors de **Reddit** y **Product Hunt**.  
2. Ranking de **Top-5 diario** por impacto (score combinado).  
3. Notificaciones automáticas vía **Telegram** y **Discord**.  

---

🛠 Inspiración

Este radar busca:

Ahorrar horas de investigación manual.

Detectar tendencias emergentes en AI, DevOps, Data, Web, Security.

Generar ideas de videos y contenido con ganchos actualizados.