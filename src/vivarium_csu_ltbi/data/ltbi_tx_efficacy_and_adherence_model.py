
# coding: utf-8

# In[1]:


import numpy as np, matplotlib.pyplot as plt, pandas as pd
pd.set_option('display.max_rows', 8)
get_ipython().system('date')

get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')


# # Model for efficacy and adherence of LTBI treatment
# 

# In[2]:


import pymc as pm


# In[3]:


df_adherence = pd.read_csv('/home/j/Project/simulation_science/latent_tuberculosis_infection'
                 '/literature/adherence_and_efficacy/treatment_adherence_tidy.csv')
df_adherence


# In[4]:


df_adherence.rct.sum()


# In[5]:


# df_adherence['fract'] = df_adherence.n_completed / df_adherence.n_enrolled
# df_adherence.loc[df_adherence.fract > .5, 'rct'] = 1 # just for testing!


# In[6]:


df_incidence = pd.read_csv('/home/j/Project/simulation_science/latent_tuberculosis_infection'
                 '/literature/adherence_and_efficacy/treatment_efficacy_tidy.csv')
df_incidence


# In[7]:


# fixed-effects model of adherence
def adherence_model(df):
    beta = [pm.Uninformative('beta_0', value=0),  # constant
            pm.Uniform('beta_1', -10, 0, value=0),  # treatment length effect (continuous, unit change = 1 month)
            pm.Uninformative('beta_2', value=0),  # HP effect (0 = daily dose, 1 = weekly dose)
            pm.Uniform('beta_3', 0, 10, value=0), # RCT effect (0 = real world, 1 = RCT)
           ]
    
    sigma = pm.Uniform('sigma', .001, 1, value=.1)
    u = np.array([pm.Normal(f'u_{i}', 0, tau=sigma**-2, value=0) for i in range(df.study.nunique())])
    
    @pm.deterministic
    def pi(beta=beta, u=u):
        return pm.invlogit(beta[0] + beta[1]*df.months + beta[2]*df.hp + beta[3]*df.rct + u[df.study])
    
    u_pred = pm.Normal('u_pred', 0, tau=sigma**-2, value=0)
    @pm.deterministic
    def pi_pred(beta=beta, u_pred=u_pred):
        return pm.invlogit(beta[0] + beta[1]*np.array([3,6,6])
                           + beta[2]*np.array([1,0,0])
                           + beta[3]*np.array([0,0,1])
                           + u_pred)
    y = pm.Binomial('y', n=df.n_enrolled, p=pi,
                    value=df.n_completed, observed=True)
    return locals()

m = pm.MCMC(adherence_model(df_adherence))
m.sample(10_000, 5_000, 5)
m.pi_pred.summary()


# In[8]:


m.beta[1].summary()
m.beta[2].summary()
m.beta[3].summary()


# In[9]:


def incidence_model(f_A):
    RR_NA = pm.Uniform('RR_NA', 1., 10., value=2)
    # latent variable : per-protocol incidence rate
    i0 = [pm.Uniform(f'i0_{j}', 0., 1., value=.01) for j in range(len(df_incidence))] # allows for different incidence rates in all arms of all studies

    pp_cases = pm.Binomial('pp_cases', df_incidence.pp_n, i0,
                           value=df_incidence.pp_c, observed=True)
    na_n = df_incidence.itt_n - df_incidence.pp_n
    na_c = df_incidence.itt_c-df_incidence.pp_c
    na_cases = pm.Binomial('na_cases', na_n, RR_NA*i0,
                           value=na_c, observed=True)
    
    
    e_ITT = pm.TruncatedNormal('itt_efficacy', mu=0.41, tau=((.80 - .19)/4)**-2, a=0.05, b=.95)
                    # consider transforming this to log or logit space

    @pm.deterministic
    def RR_no_tx(f_A=f_A, RR_NA=RR_NA, e_ITT=e_ITT):
        return (f_A + (1 - f_A) * RR_NA) / e_ITT

    return locals()

def joint_model():
    vars1 = adherence_model(df_adherence)
    f_A = vars1['pi_pred'][2] # predicted adherence fraction for an RCT of 6H
    
    vars2 = incidence_model(f_A)
    vars1.update(vars2)
    return vars1

m = pm.MCMC(joint_model())
m.sample(20_000, 10_000, 10)
m.pi_pred.summary()


# In[10]:


m.e_ITT.summary() # should be similar to prior of 0.41 (.19, .80)


# In[11]:


plt.plot(m.pi_pred.trace())
plt.xlabel('Draw from approximate posterior distribution')
plt.grid();


# In[12]:


plt.plot(m.RR_NA.trace())
plt.plot(m.RR_no_tx.trace())
plt.xlabel('Draw from approximate posterior distribution')
plt.grid();


# In[13]:


plt.plot(m.e_ITT.trace())
plt.xlabel('Draw from approximate posterior distribution')
plt.grid();


# In[14]:


for i0_j in m.i0:
    plt.plot(i0_j.trace())
plt.xlabel('Draw from approximate posterior distribution')
plt.grid();


# In[15]:


results = pd.DataFrame()
results['adherence_3hp_real_world'] = m.pi_pred.trace()[:,0]
results['adherence_6h_real_world'] = m.pi_pred.trace()[:,1]
results['adherence_6h_rct'] = m.pi_pred.trace()[:,2]
results['RR_no_tx'] = m.RR_no_tx.trace()
results['RR_NA'] = m.RR_NA.trace()
np.round(results.describe(percentiles=[0.025, 0.975]).loc[['mean', '2.5%', '97.5%']], 3).T


# In[16]:


# rr_no_tx directly from Comstock 1979
(42/1550) / (22/1633)


# In[17]:


import seaborn as sns

def y_eq_x_line(g):

    x0, x1 = g.ax_joint.get_xlim()
    y0, y1 = g.ax_joint.get_ylim()
    lims = [max(x0, y0), min(x1, y1)]
    g.ax_joint.plot(lims, lims, ':k')    
    g.ax_joint.text(x1, y1, '\ny=x    ', ha='right', va='top')

g = sns.jointplot('adherence_3hp_real_world', 'adherence_6h_real_world',
                  results, kind="kde", height=7, space=0)
y_eq_x_line(g)


# In[18]:


g = sns.jointplot('adherence_6h_rct', 'adherence_6h_real_world',
                  results, kind="kde", height=7, space=0)
y_eq_x_line(g)  # should i have a prior that the rct has better adherence than the real-world?


# In[19]:


g = sns.jointplot('RR_no_tx', 'RR_NA',
                  results, kind="kde", height=7, space=0)


# In[20]:


results.to_csv('/home/j/Project/simulation_science/latent_tuberculosis_infection/treatment_adherence_draws.csv')
results


# In[21]:


(results.RR_NA > 1).mean()


# In[22]:


(results.RR_no_tx > 1).mean()


# In[23]:


(results.RR_no_tx > results.RR_NA).mean()  # is this a problem?


# In[24]:


(results.adherence_3hp_real_world > results.adherence_6h_real_world).mean()


# In[25]:


m.beta[0].summary()
m.beta[1].summary()
m.beta[2].summary()
m.beta[3].summary()

# beta[0] is constant
# beta[1] is length effect (units are months)
# beta[2] is the 3hp effect (weekly treatment instead of daily)
# beta[3] is the rct effect

