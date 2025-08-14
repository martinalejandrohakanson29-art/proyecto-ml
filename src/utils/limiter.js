// Límite de concurrencia para no quemar requests a la API
import pLimit from 'p-limit';

// Ajustá el número según tu preferencia (5 concurrentes está bien para empezar)
export const limit = pLimit(5);
