package com.github.segabriel.agent;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.junit.jupiter.api.Test;

class DynamicCompositeAgentTest {
  private static final String ROLE_NAME = "roleName";

  @Test
  void shouldNotAllowAddAfterClose() {
    DynamicCompositeAgent compositeAgent = new DynamicCompositeAgent(ROLE_NAME);
    AgentInvoker invoker = new AgentInvoker(Throwable::printStackTrace, null, compositeAgent);

    invoker.close();

    assertThrows(IllegalStateException.class, () -> compositeAgent.add(mock(Agent.class)));
  }

  @Test
  void shouldNotAllowRemoveAfterClose() {
    DynamicCompositeAgent compositeAgent = new DynamicCompositeAgent(ROLE_NAME);
    AgentInvoker invoker = new AgentInvoker(Throwable::printStackTrace, null, compositeAgent);

    invoker.close();

    assertThrows(IllegalStateException.class, () -> compositeAgent.remove(mock(Agent.class)));
  }

  @Test
  void shouldAddAgent() throws Exception {
    Agent mockAgentOne = mock(Agent.class);

    DynamicCompositeAgent compositeAgent = new DynamicCompositeAgent(ROLE_NAME, mockAgentOne);
    AgentInvoker invoker = new AgentInvoker(Throwable::printStackTrace, null, compositeAgent);

    assertThat(compositeAgent.roleName(), is(ROLE_NAME));
    invoker.start();
    verify(mockAgentOne, times(1)).onStart();

    invoker.invoke();
    verify(mockAgentOne, times(1)).onStart();
    verify(mockAgentOne, times(1)).doWork();

    final Agent mockAgentTwo = mock(Agent.class);
    compositeAgent.add(mockAgentTwo);

    invoker.invoke();
    verify(mockAgentOne, times(1)).onStart();
    verify(mockAgentOne, times(2)).doWork();
    verify(mockAgentTwo, times(1)).onStart();
    verify(mockAgentTwo, times(1)).doWork();
  }

  @Test
  void shouldRemoveAgent() throws Exception {
    Agent mockAgentOne = mock(Agent.class);
    Agent mockAgentTwo = mock(Agent.class);

    DynamicCompositeAgent compositeAgent =
        new DynamicCompositeAgent(ROLE_NAME, mockAgentOne, mockAgentTwo);
    AgentInvoker invoker = new AgentInvoker(Throwable::printStackTrace, null, compositeAgent);
    invoker.start();
    verify(mockAgentOne, times(1)).onStart();
    verify(mockAgentTwo, times(1)).onStart();

    invoker.invoke();
    verify(mockAgentOne, times(1)).doWork();
    verify(mockAgentTwo, times(1)).doWork();

    compositeAgent.remove(mockAgentTwo);

    invoker.invoke();
    verify(mockAgentOne, times(2)).doWork();
    verify(mockAgentTwo, times(1)).doWork();
    verify(mockAgentOne, times(1)).onStart();
    verify(mockAgentTwo, times(1)).onStart();
    verify(mockAgentTwo, times(1)).onClose();
  }

  @Test
  void shouldCloseAgents() throws Exception {
    Agent mockAgentOne = mock(Agent.class);
    Agent mockAgentTwo = mock(Agent.class);

    DynamicCompositeAgent compositeAgent =
        new DynamicCompositeAgent(ROLE_NAME, mockAgentOne, mockAgentTwo);

    compositeAgent.onClose();
    verify(mockAgentOne, never()).doWork();
    verify(mockAgentTwo, never()).doWork();
    verify(mockAgentOne, never()).onStart();
    verify(mockAgentTwo, never()).onStart();
    verify(mockAgentOne, times(1)).onClose();
    verify(mockAgentTwo, times(1)).onClose();

    assertEquals(DynamicCompositeAgent.Status.CLOSED, compositeAgent.status());
  }
}
