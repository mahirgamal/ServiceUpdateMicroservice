package com.function;

import com.microsoft.azure.functions.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class FunctionTest {

    @Test
    public void testUpdateService() throws Exception {
        // Setup
        @SuppressWarnings("unchecked")
        final HttpRequestMessage<Optional<User>> req = mock(HttpRequestMessage.class);

        User testUser = new User(); // Assuming User is a POJO with relevant fields
        testUser.setEmail("test@example.com");
        testUser.setPassword("password");

        doReturn(Optional.of(testUser)).when(req).getBody();

        final Map<String, String> headers = new HashMap<>();
        headers.put("authorization", "Basic " + Base64.getEncoder().encodeToString("username:password".getBytes()));
        doReturn(headers).when(req).getHeaders();

        // Mock the HttpResponseMessage.Builder
        final HttpResponseMessage.Builder responseBuilder = mock(HttpResponseMessage.Builder.class, RETURNS_SELF);
        doReturn(mock(HttpResponseMessage.class)).when(responseBuilder).build();

        // Setup createResponseBuilder to return our mocked response builder
        doReturn(responseBuilder).when(req).createResponseBuilder(any(HttpStatus.class));

        final ExecutionContext context = mock(ExecutionContext.class);
        doReturn(Logger.getGlobal()).when(context).getLogger();

        // Instantiate your Function class
        Function function = new Function();

        // Invoke
        final HttpResponseMessage ret = function.run(req, 1L, context);

        // Verify
        assertNotNull(ret, "The response should not be null.");
        verify(req).createResponseBuilder(any(HttpStatus.class));
    }
}
