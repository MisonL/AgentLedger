export interface AuthContext {
  userId: string;
  email: string;
  displayName: string;
  tenantId: string;
  role: string;
  sessionId?: string;
}

export interface AppVariables {
  requestId: string;
  auth?: AuthContext;
}

export type AppEnv = {
  Variables: AppVariables;
};
